-- Simple denormalized query for simulation
-- Denormalized
EXPLAIN ANALYZE
SELECT
    scrape_id,
    COUNT(DISTINCT id) AS contagem_listings,
    COUNT(DISTINCT picture_url) AS contagem_fotos_url,
    COUNT(DISTINCT host_name) AS contagem_unicas_de_nomes_de_host,
    COUNT(DISTINCT neighbourhood) AS contagem_bairros_unicos,
    AVG(review_scores_rating) AS average_review_scores_rating

FROM read_csv_auto('data-airbnb/csv_bronze_listings.csv')

WHERE scrape_id IN ('20190313042552','20180414160018')
GROUP BY scrape_id
ORDER BY scrape_id;

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
EXPLAIN ANALYZE SELECT     scrape_id,     COUNT(DISTINCT id) AS contagem_listings,     COUNT(DISTINCT picture_url) AS contagem_fotos_url,     COUNT(DISTINCT host_name) AS contagem_unicas_de_nomes_de_host,     COUNT(DISTINCT neighbourhood) AS contagem_bairros_unicos,     AVG(review_scores_rating) AS average_review_scores_rating  FROM read_csv_auto('data-airbnb/csv_bronze_listings.csv')  WHERE scrape_id IN ('20190313042552','20180414160018') GROUP BY scrape_id ORDER BY scrape_id;
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││         Total Time: 2.94s         ││
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
│read_csv_auto.scrape_id ASC│
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
│          (0.04s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         scrape_id         │
│             id            │
│        picture_url        │
│         host_name         │
│       neighbourhood       │
│    review_scores_rating   │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           76121           │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│((CAST(scrape_id AS VARCHAR│
│ ) = '20190313042552') OR  │
│(CAST(scrape_id AS VAR...  │
│     '20180414160018'))    │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           76121           │
│          (0.09s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│READ_CSV_AUTO (MULTI-T...  │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│         scrape_id         │
│             id            │
│        picture_url        │
│         host_name         │
│       neighbourhood       │
│    review_scores_rating   │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           EC: 0           │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           902210          │
│          (23.22s)         │
└───────────────────────────┘ */
