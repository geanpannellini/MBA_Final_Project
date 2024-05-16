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
