package ldbc.finbench.datagen.generator.serializers

import ldbc.finbench.datagen.entities.nodes.{Company, Person}
import ldbc.finbench.datagen.generator.events.{
  CompanyGuaranteeEvent,
  CompanyLoanEvent,
  CompanyRegisterEvent,
  PersonGuaranteeEvent,
  PersonInvestEvent,
  PersonLoanEvent,
  PersonRegisterEvent,
  SignInEvent,
  TransferEvent,
  WithdrawEvent,
  WorkInEvent
}
import org.apache.spark.rdd.RDD

/**
  * generate person and company activities
  */
class ActivitySerializer {

  var personRegisterGenerator: PersonRegisterEvent     = _
  var companyRegisterGenerator: CompanyRegisterEvent   = _
  var personInvestGenerator: PersonInvestEvent         = _
  var companyInvestGenerator: CompanyRegisterEvent     = _
  var workInGenerator: WorkInEvent                     = _
  var signGenerator: SignInEvent                       = _
  var personGuaranteeGenerator: PersonGuaranteeEvent   = _
  var companyGuaranteeGenerator: CompanyGuaranteeEvent = _
  var personLoanGenerator: PersonLoanEvent             = _
  var companyLoanGenerator: CompanyLoanEvent           = _
  var transferGenerator: TransferEvent                 = _
  var withdrawGenerator: WithdrawEvent                 = _

  def personRegisterEvent(personRDD: RDD[Person]): Unit = {}

  def companyRegisterEvent(companyRDD: RDD[Company]): Unit = {}

  def personInvestEvent(): Unit = {}

  def companyInvestEvent(): Unit = {}

  def workInEvent(): Unit = {}

  def signEvent(): Unit = {}

  def personGuaranteeEvent(): Unit = {}

  def companyGuaranteeEvent(): Unit = {}

  def personLoanEvent(): Unit = {}

  def companyLoanEvent(): Unit = {}

  def transferEvent(): Unit = {}

  def withdrawEvent(): Unit = {}

}
