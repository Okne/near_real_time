create keyspace spark_hw_2 with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

create table hot_logs_table (
    bid_id text primary key,
    timestmp text,
    ipinyou_id text,
    user_agent text,
    ip text,
    region text,
    city text,
    ad_exchange text,
    domain text,
    url text,
    anonymous_url_id text,
    ad_slot_id text,
    ad_slot_width int,
    ad_slot_height int,
    ad_slot_visibility int,
    ad_slot_format int,
    paying_price double,
    creative_id text,
    bidding_price double,
    advertiser_id text,
    user_tags text,
    stream_id text
);

create table dwh_logs_table (
    bid_id text primary key,
    timestmp text,
    ipinyou_id text,
    user_agent map<text, text>,
    ip text,
    region_info map<text, text>,
    city_info map<text, text>,
    ad_exchange_info map<text, text>,
    domain text,
    url text,
    anonymous_url_id text,
    ad_slot_id text,
    ad_slot_width int,
    ad_slot_height int,
    ad_slot_visibility int,
    ad_slot_format int,
    paying_price double,
    creative_id text,
    bidding_price double,
    advertiser_id text,
    user_tags map<text, text>,
    stream map<text, text>
);

create table session_log_tokens (
    session_id text,
    start timestamp,
    end timestamp,
    tags map<text, int>,
    PRIMARY KEY(session_id, start)
);