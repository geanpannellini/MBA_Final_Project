-- Simple denormalized query for simulation
-- Denormalized
EXPLAIN ANALYZE
SELECT
    a.scrape_id,
    COUNT(DISTINCT a.id) AS contagem_listings,
    COUNT(DISTINCT b.picture_url) AS contagem_fotos_url,
    COUNT(DISTINCT c.host_name) AS contagem_unicas_de_nomes_de_host,
    COUNT(DISTINCT d.neighbourhood) AS contagem_bairros_unicos,
    AVG(average_review.average_review_scores_rating) AS average_review_scores_rating

FROM read_csv_auto('data-airbnb/csv_silver_listings.csv') AS a
LEFT JOIN
read_csv_auto('data-airbnb/csv_silver_media.csv') AS b
ON a.media_id = b.id
LEFT JOIN
read_csv_auto('data-airbnb/csv_silver_host.csv') AS c
ON a.host_id = c.id
LEFT JOIN
read_csv_auto('data-airbnb/csv_silver_address.csv') AS d
ON a.address_id = d.id
LEFT JOIN (
    SELECT
        a.scrape_id,
        AVG(h.review_scores_rating) AS average_review_scores_rating
    FROM read_csv_auto('data-airbnb/csv_silver_listings.csv') AS a
    LEFT JOIN
    read_csv_auto('data-airbnb/csv_silver_review.csv') AS h
    ON a.review_id = h.id
    WHERE a.scrape_id IN ('20190313042552', '20180414160018')
    GROUP BY a.scrape_id
) AS average_review
ON a.scrape_id = average_review.scrape_id

WHERE a.scrape_id IN ('20190313042552','20180414160018')
GROUP BY a.scrape_id
ORDER BY a.scrape_id;

/* RESULTS
┌─────────────────────────────┐
│┌───────────────────────────┐│
│└───────────────────────────┘│
└─────────────────────────────┘
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
EXPLAIN ANALYZE SELECT     a.scrape_id,     COUNT(DISTINCT a.id) AS contagem_listings,     COUNT(DISTINCT b.picture_url) AS contagem_fotos_url,     COUNT(DISTINCT c.host_name) AS contagem_unicas_de_nomes_de_host,     COUNT(DISTINCT d.neighbourhood) AS contagem_bairros_unicos,     AVG(average_review.average_review_scores_rating) AS average_review_scores_rating  FROM read_csv_auto('data-airbnb/csv_silver_listings.csv') AS a LEFT JOIN read_csv_auto('data-airbnb/csv_silver_media.csv') AS b ON a.media_id = b.id LEFT JOIN read_csv_auto('data-airbnb/csv_silver_host.csv') AS c ON a.host_id = c.id LEFT JOIN read_csv_auto('data-airbnb/csv_silver_address.csv') AS d ON a.address_id = d.id LEFT JOIN (     SELECT         a.scrape_id,         AVG(h.review_scores_rating) AS average_review_scores_rating     FROM read_csv_auto('data-airbnb/csv_silver_listings.csv') AS a     LEFT JOIN     read_csv_auto('data-airbnb/csv_silver_review.csv') AS h     ON a.review_id = h.id     WHERE a.scrape_id IN ('20190313042552', '20180414160018')     GROUP BY a.scrape_id ) AS average_review ON a.scrape_id = average_review.scrape_id  WHERE a.scrape_id IN ('20190313042552','20180414160018') GROUP BY a.scrape_id ORDER BY a.scrape_id;
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││         Total Time: 3.76s         ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
┌───────────────────────────┐
│      EXPLAIN_ANALYZE      │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             0             │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│          ORDER_BY         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          ORDERS:          │
│      a.scrape_id ASC      │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             2             │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│       HASH_GROUP_BY       │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             #0            │
│     count(DISTINCT #1)    │
│     count(DISTINCT #2)    │
│     count(DISTINCT #3)    │
│     count(DISTINCT #4)    │
│          avg(#5)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│             2             │
│          (4.99s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         scrape_id         │
│             id            │
│        picture_url        │
│         host_name         │
│       neighbourhood       │
│average_review_scores_ratin│
│             g             │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          30425935         │
│          (0.06s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         HASH_JOIN         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            LEFT           │
│   scrape_id = scrape_id   │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           ├─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│          Cost: 0          │                                                                                                     │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                                     │
│          30425935         │                                                                                                     │
│          (1.16s)          │                                                                                                     │
└─────────────┬─────────────┘                                                                                                     │
┌─────────────┴─────────────┐                                                                                       ┌─────────────┴─────────────┐
│         HASH_JOIN         │                                                                                       │       HASH_GROUP_BY       │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                       │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            LEFT           │                                                                                       │             #0            │
│      address_id = id      │                                                                                       │          avg(#1)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                       │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           ├────────────────────────────────────────────────────────────────────────┐              │             2             │
│          Cost: 0          │                                                                        │              │          (0.02s)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                        │              │                           │
│          30425935         │                                                                        │              │                           │
│          (0.74s)          │                                                                        │              │                           │
└─────────────┬─────────────┘                                                                        │              └─────────────┬─────────────┘
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         HASH_JOIN         │                                                          │READ_CSV_AUTO (MULTI-T...  ││         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            LEFT           │                                                          │             id            ││         scrape_id         │
│        host_id = id       │                                                          │       neighbourhood       ││    review_scores_rating   │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           ├───────────────────────────────────────────┐              │           EC: 0           ││          1398251          │
│          Cost: 0          │                                           │              │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││          (0.00s)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                           │              │           902210          ││                           │
│          1398251          │                                           │              │          (1.55s)          ││                           │
│          (0.13s)          │                                           │              │                           ││                           │
└─────────────┬─────────────┘                                           │              └───────────────────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         HASH_JOIN         │                             │READ_CSV_AUTO (MULTI-T...  │                             │         HASH_JOIN         │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            LEFT           │                             │             id            │                             │            LEFT           │
│       media_id = id       │                             │         host_name         │                             │       review_id = id      │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           ├──────────────┐              │           EC: 0           │                             │           EC: 0           ├──────────────┐
│          Cost: 0          │              │              │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │          Cost: 0          │              │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │              │              │           43180           │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │              │
│          1398251          │              │              │          (0.11s)          │                             │          1398251          │              │
│          (0.12s)          │              │              │                           │                             │          (0.06s)          │              │
└─────────────┬─────────────┘              │              └───────────────────────────┘                             └─────────────┬─────────────┘              │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│           FILTER          ││READ_CSV_AUTO (MULTI-T...  │                                                          │           FILTER          ││READ_CSV_AUTO (MULTI-T...  │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│((CAST(scrape_id AS VARCHAR││             id            │                                                          │((CAST(scrape_id AS VARCHAR││             id            │
│ ) = '20190313042552') OR  ││        picture_url        │                                                          │ ) = '20190313042552') OR  ││    review_scores_rating   │
│(CAST(scrape_id AS VAR...  ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          │(CAST(scrape_id AS VAR...  ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│     '20180414160018'))    ││           EC: 0           │                                                          │     '20180414160018'))    ││           EC: 0           │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                          │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           ││           902210          │                                                          │           EC: 0           ││           902210          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││          (4.91s)          │                                                          │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││          (1.04s)          │
│           76121           ││                           │                                                          │           76121           ││                           │
│          (0.06s)          ││                           │                                                          │          (0.07s)          ││                           │
└─────────────┬─────────────┘└───────────────────────────┘                                                          └─────────────┬─────────────┘└───────────────────────────┘
┌─────────────┴─────────────┐                                                                                       ┌─────────────┴─────────────┐
│READ_CSV_AUTO (MULTI-T...  │                                                                                       │READ_CSV_AUTO (MULTI-T...  │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                       │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│          media_id         │                                                                                       │         review_id         │
│          host_id          │                                                                                       │         scrape_id         │
│         address_id        │                                                                                       │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         scrape_id         │                                                                                       │           EC: 0           │
│             id            │                                                                                       │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                       │           902210          │
│           EC: 0           │                                                                                       │          (8.17s)          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                                                                                       │                           │
│           902210          │                                                                                       │                           │
│          (4.85s)          │                                                                                       │                           │
└───────────────────────────┘                                                                                       └───────────────────────────┘
*/
