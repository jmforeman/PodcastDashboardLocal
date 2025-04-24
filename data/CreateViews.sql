CREATE INDEX IF NOT EXISTS idx_top100lists_lookup ON Top100Lists (platform, title, date);
CREATE INDEX IF NOT EXISTS idx_podcasts_title ON Podcasts (title);
-------------------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS vw_CurrentPodcastDetailsWithCategories AS
WITH LatestRank AS (
    -- Step 1: Find the most recent date in the Top100Lists table
    SELECT MAX(date) as latest_date
    FROM Top100Lists
),
CurrentTop100 AS (
    -- Step 2: Get the Top 100 entries only for that latest date
    SELECT
        t100.platform,
        t100.rank,
        t100.title AS list_title, -- Alias to distinguish from Podcasts.title
        t100.platform_podcast_id,
        t100.date
    FROM Top100Lists t100
    JOIN LatestRank lr ON t100.date = lr.latest_date
)
-- Step 3: Join the current Top 100 list with Podcast details and Categories
SELECT
    ct100.platform,
    ct100.rank,
    ct100.list_title,
    ct100.platform_podcast_id,
    p.podcast_id AS podcast_index_id, -- Clarify which ID this is
    p.title AS podcast_index_title, -- Alias to distinguish
    p.description,
    p.feed_url,
    p.image_url,
    p.episode_count,
    p.avg_duration_last_10,
    p.latest_episode_title,
    p.last_update_time,
    p.podcast_guid,
    p.original_url,
    c.category_id,
    c.category_name
FROM CurrentTop100 ct100
-- Step 3a: Join Top100 list to Podcast details (using title - adjust if better join exists)
-- Using LEFT JOIN in case a podcast in the Top100 hasn't had its details fetched yet
LEFT JOIN Podcasts p ON LOWER(TRIM(ct100.list_title)) = LOWER(TRIM(p.title)) -- Join on trimmed, lowercased title
-- Step 3b: Join Podcast details to the category links
LEFT JOIN PodcastCategories pc ON p.podcast_id = pc.podcast_id
-- Step 3c: Join category links to category names
LEFT JOIN Categories c ON pc.category_id = c.category_id;
-------------------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS vw_RankChanges AS
WITH RankedData AS (
    -- Use LAG window function to get the previous rank for the same podcast on the same platform
    SELECT
        date,
        platform,
        title AS list_title, -- Use original title from list for display
        rank,
        LAG(rank, 1) OVER (
            PARTITION BY platform, LOWER(TRIM(title)) -- Group by platform and cleaned title
            ORDER BY date -- Order by date to find the previous day's rank
        ) as previous_rank
    FROM Top100Lists
)
SELECT
    date,
    platform,
    list_title,
    rank AS current_rank,
    previous_rank,
    -- Calculate the change: Positive means rank improved (number decreased)
    CASE
        WHEN previous_rank IS NULL THEN NULL -- First appearance on list
        ELSE previous_rank - rank
    END as rank_change
FROM RankedData
ORDER BY
    platform,
    list_title,
    date DESC; -- Show most recent changes first
-------------------------------------------------------------------------------------	
CREATE VIEW IF NOT EXISTS vw_TimeOnList AS
SELECT
    platform,
    title AS list_title,
    COUNT(DISTINCT date) as days_on_list,
    MIN(date) as first_seen_date, -- Earliest date recorded for this podcast/platform
    MAX(date) as last_seen_date   -- Latest date recorded for this podcast/platform
FROM Top100Lists
WHERE title IS NOT NULL -- Exclude potential null titles
GROUP BY
    platform,
    LOWER(TRIM(title)) -- Group by cleaned title for consistency
ORDER BY
    days_on_list DESC,
    platform,
    list_title;
-------------------------------------------------------------------------------------	
CREATE VIEW IF NOT EXISTS vw_PlatformOverlap AS
WITH LatestDate AS (
    -- Find the most recent date overall
    SELECT MAX(date) as latest_date
    FROM Top100Lists
),
AppleList_Latest AS (
    -- Get distinct cleaned titles from Apple for the latest date
    SELECT DISTINCT LOWER(TRIM(title)) as cleaned_title
    FROM Top100Lists t100
    JOIN LatestDate ld ON t100.date = ld.latest_date
    WHERE t100.platform = 'Apple' AND t100.title IS NOT NULL
),
SpotifyList_Latest AS (
    -- Get distinct cleaned titles from Spotify for the latest date
    SELECT DISTINCT LOWER(TRIM(title)) as cleaned_title
    FROM Top100Lists t100
    JOIN LatestDate ld ON t100.date = ld.latest_date
    WHERE t100.platform = 'Spotify' AND t100.title IS NOT NULL
)
-- Find titles that exist in both lists
SELECT
    a.cleaned_title AS overlapping_title
FROM AppleList_Latest a
JOIN SpotifyList_Latest s ON a.cleaned_title = s.cleaned_title;
-------------------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS vw_NewEntries AS
WITH DateRanks AS (
    -- Assign ranks to distinct dates to easily find latest (1) and previous (2)
    SELECT DISTINCT
        date,
        DENSE_RANK() OVER (ORDER BY date DESC) as date_rank
    FROM Top100Lists
),
LatestDate AS (
    SELECT date FROM DateRanks WHERE date_rank = 1
),
PreviousDate AS (
    -- Find the date ranked just before the latest (might not be yesterday if scraping missed a day)
    SELECT date FROM DateRanks WHERE date_rank = 2
),
LatestPodcasts AS (
    -- Get all podcasts from the latest date
    SELECT platform, title, rank, LOWER(TRIM(title)) as cleaned_title
    FROM Top100Lists
    WHERE date = (SELECT date FROM LatestDate)
      AND title IS NOT NULL
),
PreviousPodcasts AS (
    -- Get distinct cleaned titles from the previous recorded date
    SELECT DISTINCT platform, LOWER(TRIM(title)) as cleaned_title
    FROM Top100Lists
    WHERE date = (SELECT date FROM PreviousDate)
      AND title IS NOT NULL
)
-- Select podcasts from the latest list that don't have a match in the previous list
SELECT
    lp.platform,
    lp.title,
    lp.rank AS current_rank
FROM LatestPodcasts lp
LEFT JOIN PreviousPodcasts pp
    ON lp.platform = pp.platform AND lp.cleaned_title = pp.cleaned_title
WHERE
    pp.cleaned_title IS NULL -- The LEFT JOIN found no match in the previous list
ORDER BY
    lp.platform,
    lp.rank;