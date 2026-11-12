/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.config.DatagenConfiguration
import ldbc.finbench.datagen.entities.edges.{Deposit, Repay, SignIn, Transfer, Withdraw}
import ldbc.finbench.datagen.entities.nodes.{
  Account, Company, InvestorInfo, Loan, LoanTargetAccount, Medium, Person, SignInTargetInfo
}
import ldbc.finbench.datagen.generation.{DatagenContext, DatagenParams}
import ldbc.finbench.datagen.generation.events._
import ldbc.finbench.datagen.generation.events.AccountActivitiesEvent.WithdrawCard
import ldbc.finbench.datagen.util.{Logging, RandomGeneratorFarm}
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer

case class LoanActivityBundle(
    loanId: Long,
    creationDate: Long,
    loanAmount: Double,
    balance: Double,
    usage: String,
    interestRate: Double,
    deposits: Seq[Deposit],
    repays: Seq[Repay],
    loanTransfers: Seq[Transfer]
) extends Serializable

class ActivityGenerator(config: DatagenConfiguration)(implicit spark: SparkSession)
    extends Serializable
    with Logging {

  val blockSize: Int = DatagenParams.blockSize
  val sampleRandom = new scala.util.Random(DatagenParams.defaultSeed)
  val accountGenerator = new AccountGenerator()
  val loanGenerator = new LoanGenerator()
  private val shardCount: Int = Math.max(1, spark.sparkContext.defaultParallelism)
  private val shardPartitioner = new HashPartitioner(shardCount)
  private val neighborFanout: Int = 1

  private case class SignInRequest(
      mediumId: Long,
      mediumCreationDate: Long,
      multiplicity: Int,
      candidateSeed: Long,
      eventSeed: Long
  ) extends Serializable

  // including account, loan, guarantee
  def personActivitiesEvent(personRDD: RDD[Person]): RDD[Person] = {
    val personActivitiesEvent = new PersonActivitiesEvent
    val blocks = personRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val personWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        personByRank => SortedMap(personByRank),
        (map: SortedMap[Long, Person], personByRank) => map + personByRank,
        (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
      )
      .mapPartitions(groups => {
        DatagenContext.initialize(config)
        groups.flatMap { case (block, persons) =>
          personActivitiesEvent
            .personActivities(
              persons.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    personWithAccountsLoansGuarantees
  }

  def companyActivitiesEvent(companyRDD: RDD[Company]): RDD[Company] = {
    val companyActivitiesEvent = new CompanyActivitiesEvent
    val blocks = companyRDD.zipWithUniqueId().map(row => (row._2, row._1)).map {
      case (k, v) => (k / blockSize, (k, v))
    }

    val companyWithAccountsLoansGuarantees = blocks
      .combineByKeyWithClassTag(
        companyByRank => SortedMap(companyByRank),
        (map: SortedMap[Long, Company], companyByRank) => map + companyByRank,
        (a: SortedMap[Long, Company], b: SortedMap[Long, Company]) => a ++ b
      )
      .mapPartitions(groups => {
        DatagenContext.initialize(config)
        groups.flatMap { case (block, companies) =>
          companyActivitiesEvent
            .companyActivities(
              companies.values.toList.asJava,
              accountGenerator,
              loanGenerator,
              block.toInt
            )
            .iterator()
            .asScala
        }
      })

    companyWithAccountsLoansGuarantees
  }

  def investEvent(
      personRDD: RDD[Person],
      companyRDD: RDD[Company]
  ): RDD[Company] = {
    val personInfos = spark.sparkContext.broadcast(
      personRDD.map(p => new InvestorInfo(p.getPersonId, p.getCreationDate)).collect()
    )
    val companyInfos = spark.sparkContext.broadcast(
      companyRDD.map(c => new InvestorInfo(c.getCompanyId, c.getCreationDate)).collect()
    )

    val personInvestEvent = new PersonInvestEvent()
    val companyInvestEvent = new CompanyInvestEvent()

    companyRDD
      .sample(
        withReplacement = false,
        DatagenParams.companyInvestedFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex { (index, targets) =>
        DatagenContext.initialize(config)
        personInvestEvent.resetState(index)
        personInvestEvent
          .personInvestPartition(personInfos.value, targets.toList.asJava)
          .iterator()
          .asScala
      }
      .mapPartitionsWithIndex { (index, targets) =>
        DatagenContext.initialize(config)
        companyInvestEvent.resetState(index)
        companyInvestEvent
          .companyInvestPartition(
            companyInfos.value,
            targets.toList.asJava
          )
          .iterator()
          .asScala
      }
      .map(_.scaleInvestmentRatios())
  }

  def mediumActivitesEvent(
      mediumRDD: RDD[Medium],
      accountRDD: RDD[Account]
  ): RDD[Medium] = {
    val mediumPartitionCount = mediumRDD.getNumPartitions
    val partitioner = new HashPartitioner(mediumPartitionCount)
    val accountTargets = accountRDD
      .sample(
        withReplacement = false,
        DatagenParams.accountSignedInFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex { (partitionId, accounts) =>
        accounts
        .flatMap { a =>
          neighborPartitionIds(partitionId, mediumPartitionCount).map(_ -> new SignInTargetInfo(
            a.getAccountId,
            a.getCreationDate,
            a.getDeletionDate,
            a.isExplicitlyDeleted
          ))
        }
      }
      .partitionBy(partitioner)
      .values

    mediumRDD.zipPartitions(accountTargets) { (mediums, candidates) =>
      DatagenContext.initialize(config)
      val partitionId = TaskContext.getPartitionId()
      val farm = new RandomGeneratorFarm()
      farm.resetRandomGenerators(partitionId)
      val accountsToSignRand = farm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_SIGNIN_PER_MEDIUM)
      val multiplicityRand = farm.get(RandomGeneratorFarm.Aspect.MULTIPLICITY_SIGNIN)
      val candidateRand = new java.util.Random(partitionId.toLong)
      val candidateArray = candidates.toArray.sortBy(_.getAccountId)
      val numAccountsToSign = Math.max(1, accountsToSignRand.nextInt(DatagenParams.maxAccountToSignIn))

      mediums.map { medium =>
        if (candidateArray.nonEmpty) {
          var count = 0
          while (count < numAccountsToSign) {
            val target = candidateArray(candidateRand.nextInt(candidateArray.length))
            if (!cannotSignIn(medium.getCreationDate, target)) {
              val multiplicity = Math.max(1, multiplicityRand.nextInt(DatagenParams.maxSignInPerPair))
              var multiplicityId = 0
              while (multiplicityId < multiplicity) {
                SignIn.createSignIn(farm, multiplicityId, medium, target)
                multiplicityId += 1
              }
            }
            count += 1
          }
        }
        medium
      }
    }
  }

  def mediumActivitesEventWithShardShuffle(
      mediumRDD: RDD[Medium],
      accountRDD: RDD[Account]
  ): RDD[Medium] = {
    val accountTargets = accountRDD
      .sample(
        withReplacement = false,
        DatagenParams.accountSignedInFraction,
        sampleRandom.nextLong()
      )
      .map(a => shardFor(a.getAccountId) -> new SignInTargetInfo(
        a.getAccountId,
        a.getCreationDate,
        a.getDeletionDate,
        a.isExplicitlyDeleted
      ))

    val signInRequests = mediumRDD.mapPartitionsWithIndex { (partitionId, mediums) =>
      DatagenContext.initialize(config)
      val farm = new RandomGeneratorFarm()
      farm.resetRandomGenerators(partitionId)
      val accountsToSignRand = farm.get(RandomGeneratorFarm.Aspect.NUM_ACCOUNTS_SIGNIN_PER_MEDIUM)
      val multiplicityRand = farm.get(RandomGeneratorFarm.Aspect.MULTIPLICITY_SIGNIN)
      val routingRand = new java.util.Random(partitionId.toLong)
      val numAccountsToSign = Math.max(1, accountsToSignRand.nextInt(DatagenParams.maxAccountToSignIn))

      mediums.flatMap { medium =>
        (0 until numAccountsToSign).iterator.map { _ =>
          val multiplicity = Math.max(1, multiplicityRand.nextInt(DatagenParams.maxSignInPerPair))
          val shardId = routingRand.nextInt(shardCount)
          shardId -> SignInRequest(
            mediumId = medium.getMediumId,
            mediumCreationDate = medium.getCreationDate,
            multiplicity = multiplicity,
            candidateSeed = routingRand.nextLong(),
            eventSeed = routingRand.nextLong()
          )
        }
      }
    }

    val signInsByMediumId = signInRequests
      .cogroup(accountTargets, shardPartitioner)
      .flatMap { case (_, (requests, candidates)) =>
        DatagenContext.initialize(config)
        val candidateArray = candidates.iterator.toArray.sortBy(_.getAccountId)
        if (candidateArray.isEmpty) {
          Iterator.empty
        } else {
          requests.iterator.toArray.sortBy(request =>
            (request.mediumId, request.candidateSeed, request.eventSeed)
          ).iterator.flatMap { request =>
            val target = candidateArray(pickCandidateIndex(request.candidateSeed, candidateArray.length))
            if (cannotSignIn(request.mediumCreationDate, target)) {
              Iterator.empty
            } else {
              val farm = new RandomGeneratorFarm()
              farm.resetRandomGenerators(request.eventSeed)
              val medium = new Medium()
              medium.setMediumId(request.mediumId)
              medium.setCreationDate(request.mediumCreationDate)
              var multiplicityId = 0
              while (multiplicityId < request.multiplicity) {
                SignIn.createSignIn(farm, multiplicityId, medium, target)
                multiplicityId += 1
              }
              medium.getSignIns.asScala.iterator.map(signIn => request.mediumId -> signIn)
            }
          }
        }
      }
      .combineByKeyWithClassTag[ArrayBuffer[SignIn]](
        (signIn: SignIn) => ArrayBuffer(signIn),
        (buffer: ArrayBuffer[SignIn], signIn: SignIn) => {
          buffer += signIn
          buffer
        },
        (left: ArrayBuffer[SignIn], right: ArrayBuffer[SignIn]) => {
          left ++= right
          left
        }
      )

    mediumRDD
      .keyBy(_.getMediumId)
      .leftOuterJoin(signInsByMediumId)
      .values
      .map { case (medium, signInsOpt) =>
        signInsOpt.foreach(_.foreach(medium.getSignIns.add))
        medium
      }
  }

  def accountActivitiesEvent(accountRDD: RDD[Account]): RDD[Account] = {
    accountRDD.mapPartitions { accountsIter =>
      DatagenContext.initialize(config)
      val partitionId = TaskContext.getPartitionId()
      val accountActivitiesEvent = new AccountActivitiesEvent
      accountActivitiesEvent
        .accountActivities(
          accountsIter.toArray,
          Array.empty[WithdrawCard],
          partitionId
        )
        .iterator()
        .asScala
        .map { account =>
          account.getTransferIns.clear()
          account
        }
    }
  }

  def withdrawActivitiesEvent(accountRDD: RDD[Account]): RDD[Withdraw] = {
    val accountPartitionCount = accountRDD.getNumPartitions
    val partitioner = new HashPartitioner(accountPartitionCount)
    val cardsRDD: RDD[WithdrawCard] = accountRDD
      .mapPartitionsWithIndex { (partitionId, accounts) =>
        accounts
          .filter(_.getType == "debit card")
          .flatMap { a =>
            neighborPartitionIds(partitionId, accountPartitionCount).map(_ -> new WithdrawCard(
              a.getAccountId,
              a.getType,
              a.getCreationDate,
              a.getDeletionDate,
              a.isExplicitlyDeleted
            ))
          }
      }
      .partitionBy(partitioner)
      .values

    accountRDD.zipPartitions(cardsRDD) { (accounts, cards) =>
      DatagenContext.initialize(config)
      val partitionId = TaskContext.getPartitionId()
      val farm = new RandomGeneratorFarm()
      farm.resetRandomGenerators(partitionId)
      val pickAccountForWithdrawal = farm.get(RandomGeneratorFarm.Aspect.ACCOUNT_WHETHER_WITHDRAW)
      val cardIndexRand = new java.util.Random(partitionId.toLong)
      val cardArray = cards.toArray.sortBy(_.getAccountId)
      val multiplicityMap = scala.collection.mutable.HashMap.empty[(Long, Long), Long]

      accounts.flatMap { account =>
        if (
          cardArray.isEmpty ||
          account.getType == "debit card" ||
          pickAccountForWithdrawal.nextDouble() >= DatagenParams.accountWithdrawFraction
        ) {
          Iterator.empty
        } else {
          (0 until DatagenParams.maxWithdrawals).iterator.flatMap { _ =>
            val target = cardArray(cardIndexRand.nextInt(cardArray.length))
            if (cannotWithdraw(account, target)) {
              Iterator.empty
            } else {
              Iterator.single(
                createWithdraw(
                  account,
                  target,
                  farm,
                  nextMultiplicity(multiplicityMap, account.getAccountId, target.getAccountId)
                )
              )
            }
          }
        }
      }
    }
  }

  def afterLoanSubEvents(
      loanRDD: RDD[Loan],
      accountRDD: RDD[Account]
  ): RDD[Loan] = {
    loanActivitiesEvent(loanRDD, accountRDD).map { bundle =>
      val loan = new Loan(
        bundle.loanId,
        bundle.loanAmount,
        bundle.balance,
        bundle.creationDate,
        0L,
        bundle.usage,
        bundle.interestRate
      )
      bundle.deposits.foreach(loan.getDeposits.add)
      bundle.repays.foreach(loan.getRepays.add)
      bundle.loanTransfers.foreach(loan.addLoanTransfer)
      loan
    }
  }

  def loanActivitiesEvent(
      loanRDD: RDD[Loan],
      accountRDD: RDD[Account]
  ): RDD[LoanActivityBundle] = {
    val loanPartitionCount = loanRDD.getNumPartitions
    val partitioner = new HashPartitioner(loanPartitionCount)
    val transferTargets: RDD[LoanTargetAccount] = accountRDD
      .sample(
        withReplacement = false,
        DatagenParams.loanInvolvedAccountsFraction,
        sampleRandom.nextLong()
      )
      .mapPartitionsWithIndex { (partitionId, accounts) =>
        accounts.flatMap { a =>
          neighborPartitionIds(partitionId, loanPartitionCount).map(_ -> new LoanTargetAccount(
            a.getAccountId,
            a.getCreationDate,
            a.getDeletionDate,
            a.isExplicitlyDeleted
          ))
        }
      }
      .partitionBy(partitioner)
      .values

    require(
      loanPartitionCount == transferTargets.getNumPartitions,
      s"loanRDD partitions ($loanPartitionCount) must match account target partitions (${transferTargets.getNumPartitions})"
    )

    loanRDD.zipPartitions(transferTargets) { (loans, targets) =>
      DatagenContext.initialize(config)
      val partitionId = TaskContext.getPartitionId()
      val farm = new RandomGeneratorFarm()
      farm.resetRandomGenerators(partitionId)
      val indexRand = new java.util.Random(partitionId.toLong)
      val actionRand = new java.util.Random(17L * partitionId + 7L)
      val amountRand = new java.util.Random(31L * partitionId + 11L)
      val targetArray = targets.toArray.sortBy(_.getAccountId)
      val multiplicityMap = scala.collection.mutable.HashMap.empty[(Long, Long), Long]

      loans.map { loan =>
        var count = 0
        while (count < DatagenParams.numLoanActions) {
          actionRand.nextInt(3) match {
            case 0 =>
              withLoanAccount(loan, indexRand) { account =>
                if (!cannotDeposit(loan, account)) {
                  Deposit.createDeposit(farm, loan, account, amountRand.nextDouble() * loan.getBalance)
                }
              }
            case 1 =>
              withLoanAccount(loan, indexRand) { account =>
                if (!cannotRepay(account, loan)) {
                  Repay.createRepay(
                    farm,
                    account,
                    loan,
                    amountRand.nextDouble() * (loan.getLoanAmount - loan.getBalance)
                  )
                }
              }
            case _ =>
              withLoanAccount(loan, indexRand) { account =>
                if (targetArray.nonEmpty) {
                  val target = targetArray(indexRand.nextInt(targetArray.length))
                  val forward = actionRand.nextDouble() < 0.5
                  val amount = amountRand.nextDouble() * DatagenParams.transferMaxAmount
                  if (forward) {
                    if (!cannotTransfer(account, target)) {
                      Transfer.createLoanTransfer(
                        farm,
                        account,
                        target,
                        loan,
                        nextMultiplicity(multiplicityMap, account.getAccountId, target.getAccountId),
                        amount
                      )
                    }
                  } else {
                    if (!cannotTransfer(target, account)) {
                      Transfer.createLoanTransfer(
                        farm,
                        target,
                        account,
                        loan,
                        nextMultiplicity(multiplicityMap, target.getAccountId, account.getAccountId),
                        amount
                      )
                    }
                  }
                }
              }
          }
          count += 1
        }
        LoanActivityBundle(
          loanId = loan.getLoanId,
          creationDate = loan.getCreationDate,
          loanAmount = loan.getLoanAmount,
          balance = loan.getBalance,
          usage = loan.getUsage,
          interestRate = loan.getInterestRate,
          deposits = loan.getDeposits.asScala.toVector,
          repays = loan.getRepays.asScala.toVector,
          loanTransfers = loan.getLoanTransfers.asScala.toVector
        )
      }
    }
  }

  private def shardFor(entityId: Long): Int =
    Math.floorMod(java.lang.Long.hashCode(entityId), shardCount)

  private def neighborPartitionIds(partitionId: Int, partitionCount: Int): Iterator[Int] = {
    if (partitionCount <= 0) {
      Iterator.empty
    } else {
      val normalized = Math.floorMod(partitionId, partitionCount)
      Iterator
        .range(-neighborFanout, neighborFanout + 1)
        .map(offset => Math.floorMod(normalized + offset, partitionCount))
        .toSeq
        .distinct
        .iterator
    }
  }

  private def pickCandidateIndex(seed: Long, candidateCount: Int): Int = {
    val rand = new java.util.Random(seed)
    rand.nextInt(candidateCount)
  }

  private def cannotSignIn(mediumCreationDate: Long, account: SignInTargetInfo): Boolean =
    mediumCreationDate + DatagenParams.activityDelta > account.getDeletionDate

  private def cannotDeposit(loan: Loan, account: Account): Boolean =
    loan.getBalance == 0 || loan.getCreationDate + DatagenParams.activityDelta > account.getDeletionDate

  private def cannotRepay(account: Account, loan: Loan): Boolean =
    loan.getLoanAmount == loan.getBalance ||
      account.getDeletionDate < loan.getCreationDate + DatagenParams.activityDelta

  private def cannotTransfer(from: Account, to: LoanTargetAccount): Boolean =
    from.getDeletionDate < to.getCreationDate + DatagenParams.activityDelta ||
      from.getCreationDate + DatagenParams.activityDelta > to.getDeletionDate

  private def cannotTransfer(from: LoanTargetAccount, to: Account): Boolean =
    from.getDeletionDate < to.getCreationDate + DatagenParams.activityDelta ||
      from.getCreationDate + DatagenParams.activityDelta > to.getDeletionDate

  private def cannotWithdraw(from: Account, to: WithdrawCard): Boolean =
    from.getType == "debit card" ||
      from.getDeletionDate < to.getCreationDate + DatagenParams.activityDelta ||
      from.getCreationDate + DatagenParams.activityDelta > to.getDeletionDate ||
      from.getAccountId == to.getAccountId

  private def withLoanAccount(
      loan: Loan,
      random: java.util.Random
  )(f: Account => Unit): Unit = {
    val accounts = loan.getAccounts
    if (accounts != null && accounts.nonEmpty) {
      f(accounts(random.nextInt(accounts.length)))
    }
  }

  private def nextMultiplicity(
      multiplicityMap: scala.collection.mutable.HashMap[(Long, Long), Long],
      fromAccountId: Long,
      toAccountId: Long
  ): Long = {
    val key = fromAccountId -> toAccountId
    val value = multiplicityMap.getOrElse(key, 0L)
    multiplicityMap.update(key, value + 1L)
    value
  }

  private def createWithdraw(
      source: Account,
      target: WithdrawCard,
      farm: RandomGeneratorFarm,
      multiplicityId: Long
  ): Withdraw = {
    val from = new Account()
    from.setAccountId(source.getAccountId)
    from.setType(source.getType)
    from.setCreationDate(source.getCreationDate)
    from.setDeletionDate(source.getDeletionDate)
    from.setExplicitlyDeleted(source.isExplicitlyDeleted)
    Withdraw.createWithdraw(
      farm,
      from,
      target.getAccountId,
      target.getType,
      target.getCreationDate,
      target.getDeletionDate,
      target.isExplicitlyDeleted,
      multiplicityId
    )
    from.getWithdraws.get(0)
  }
}
