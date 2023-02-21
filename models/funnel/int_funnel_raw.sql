{{
    config(
        materialized='table'
    )
}}

-- Facebook consolidation 
select 
    DATE,
    DATA_SOURCE_TYPE, 
    CAMPAIGN_NAME__FACEBOOK_ADS AS campaign, 
    DATA_SOURCE_NAME,
    CURRENCY,
    COST, 
    CLICKS,
    IMPRESSIONS

from {{ ref('stg_funnel_raw') }}
where DATA_SOURCE_TYPE = 'facebookads'

UNION ALL 

-- Google Ads consolidation 
select 
    DATE,
    DATA_SOURCE_TYPE, 
    CAMPAIGN__ADWORDS AS campaign, 
    DATA_SOURCE_NAME,
    CURRENCY,
    COST, 
    CLICKS,
    IMPRESSIONS

from {{ ref('stg_funnel_raw') }}
where DATA_SOURCE_TYPE = 'adwords'

UNION ALL

-- DV360 consolidation 
select 
    DATE,
    DATA_SOURCE_TYPE, 
    CAMPAIGN__DOUBLECLICK_BID_MANAGER AS campaign, 
    DATA_SOURCE_NAME,
    CURRENCY,
    COST, 
    CLICKS,
    IMPRESSIONS
from {{ ref('stg_funnel_raw') }}
where DATA_SOURCE_TYPE = 'doubleclick_bidmanager_api'

UNION ALL

-- Linkedin consolidation 
select 
    DATE,
    DATA_SOURCE_TYPE, 
    CAMPAIGN__LINKEDIN AS campaign, 
    DATA_SOURCE_NAME,
    CURRENCY,
    COST, 
    CLICKS,
    IMPRESSIONS
from {{ ref('stg_funnel_raw') }}
where DATA_SOURCE_TYPE = 'linkedin_api'

order by DATE DESC