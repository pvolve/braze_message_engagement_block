# Campaign Conversions
view: users_campaigns_conversion {
  derived_table: {
    sql:
      with conversions as (
        select * from DATALAKE_SHARING.USERS_CAMPAIGNS_CONVERSION_SHARED
      ),
      campaign as (
        select id as campaign_id,
        name as campaign_name,
        time as updated_timestamp
      from DATALAKE_SHARING.CHANGELOGS_CAMPAIGN_SHARED
      ),
      joined as (
        select conversions.*, campaign_name
        FROM conversions
        LEFT JOIN campaign
        ON conversions.campaign_id = campaign.campaign_id
        AND time >= updated_timestamp
        qualify row_number() over (partition by conversions.id ORDER BY updated_timestamp DESC) = 1
      )
      select * from joined
      ;;
  }


  dimension: id {
    primary_key: yes
    description: "unique id of this event"
    hidden: yes
    type: string
    sql: ${TABLE}."ID" ;;
  }

  dimension: app_id {
    description: "id of the app. This is App API ID"
    type: string
    sql: ${TABLE}."APP_API_ID" ;;
  }

  dimension: campaign_id {
    description: "id of the campaign"
    type: string
    sql: ${TABLE}."CAMPAIGN_ID" ;;
  }

  dimension: campaign_name {
    description: "name of the campaign"
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

# the below dimensions use Snowflake syntax to turn a json string into code that can be queried.
# use the proper code for your SQL dialect.
  dimension: conversion_behavior {
    description: "JSON-encoded string describing the conversion behavior. This is not making it in from Snowflake"
    hidden: yes
    type: string
    sql: parse_json(${TABLE}."CONVERSION_BEHAVIOR");;
  }

  dimension: conversion_behavior_index {
    description: "index of the conversion behavior"
    hidden: yes
    type: number
    sql: ${TABLE}."CONVERSION_BEHAVIOR_INDEX" ;;
  }

  dimension: conversion_behavior_type {
    description: "type of the conversion behavior. This is not making it in from Snowflake"
    type: string
    hidden: yes
    sql: ${conversion_behavior}:type::STRING ;;
  }

  dimension: conversion_window {
    description: "window of time to perform the conversion behavior in seconds"
    type: number
    hidden: yes
    sql: ${conversion_behavior}:window::NUMBER ;;
  }

  dimension: conversion_custom_event_name {
    description: "if the conversion was a custom event, the name of custom event"
    type: string
    hidden: yes
    sql: ${conversion_behavior}:custom_event_name::STRING ;;
  }

  dimension_group: converted_time {
    description: "timestamp of the conversion"
    type: time
    hidden: no
    datatype: epoch
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}."TIME" ;;
  }

  dimension: converted_timezone {
    description: "IANA timezone of the user at the time of the event"
    hidden: no
    type: string
    sql: ${TABLE}."TIMEZONE" ;;
  }

  dimension: external_user_id {
    description: "External ID of the user"
    type: string
    hidden: no
    sql: ${TABLE}."EXTERNAL_USER_ID" ;;
  }

  dimension: message_variation_id {
    description: "id of the message variation if from a campaign"
    type: string
    hidden: no
    sql: ${TABLE}."MESSAGE_VARIATION_API_ID" ;;
  }

  dimension: send_id {
    description: "id of the message if specified for the campaign"
    hidden: no
    type: string
    sql: ${TABLE}."SEND_ID" ;;
  }

  dimension: user_id {
    description: "Braze user ID"
    type: string
    hidden: yes
    sql: ${TABLE}."USER_ID" ;;
  }

  dimension: user_in_control {
    description: "is this user in the control group for this campaign"
    type: yesno
    hidden: no
    sql: ${users_campaigns_enrollincontrol.user_id}=${user_id}
         AND
         ${users_campaigns_enrollincontrol.message_variation_id}=${message_variation_id};;
  }

  measure: campaign_conversion_event_count {
    description: "distinct count of campaign conversion behavior event IDs"
    type: count_distinct
    sql: ${TABLE}."ID" ;;
  }

  measure: unique_users_that_converted_on_a_campaign {
    description: "distinct count of Braze user IDs who converted on a campaign"
    type: count_distinct
    sql: ${TABLE}."USER_ID" ;;
  }
}
