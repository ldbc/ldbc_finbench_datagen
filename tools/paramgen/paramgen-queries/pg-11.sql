SELECT
    personId AS 'personId',
    countryName AS 'countryName',
    1998 + salt * 37 % 15 AS 'workFromYear', -- 1998..2013
    useFrom AS 'useFrom',
    useUntil AS 'useUntil'
FROM
    (
        SELECT Person1Id AS personId,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendsSelected
         WHERE deletionDate - INTERVAL 1 DAY > :date_limit_filter
           AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY diff, md5(Person1Id)
         LIMIT 50
    ),
    (
        SELECT name AS countryName,
               frequency AS freq,
               abs(frequency - (
                   SELECT percentile_disc(0.2)
                   WITHIN GROUP (ORDER BY frequency)
                   FROM countryNumPersons)
               ) AS diff
          FROM countryNumPersons
         ORDER BY diff, countryName
         LIMIT 20
    ),
    (
        SELECT unnest(generate_series(1, 20)) AS salt
    )
ORDER BY md5(concat(personId, countryName, salt))
LIMIT 500
