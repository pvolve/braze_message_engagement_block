# Email Messaging Cadence
view: email_messaging_cadence {
  derived_table: {
    sql: with deliveries as
      (select TO_TIMESTAMP(time) AS delivered_timestamp,
      email_address AS delivered_address,
      message_variation_api_id as d_message_variation_api_id,
      canvas_step_api_id as d_canvas_step_api_id,
      campaign_id as d_campaign_id,
      canvas_id as d_canvas_id,
      id as delivered_id,
      rank() over (partition by delivered_address order by delivered_timestamp asc) as delivery_event,
      min(delivered_timestamp) over (partition by delivered_address order by delivered_timestamp asc) as first_delivered,
      datediff(day, lag(delivered_timestamp) over (partition by delivered_address order by delivered_timestamp asc), delivered_timestamp) as diff_days,
      datediff(week, lag(delivered_timestamp) over (partition by delivered_address order by delivered_timestamp asc), delivered_timestamp) as diff_weeks
      from DATALAKE_SHARING.USERS_MESSAGES_EMAIL_DELIVERY_SHARED group by 1,2,3,4,5,6,7),

      opens as (
        select
          distinct email_address as open_address,
          message_variation_api_id as o_message_variation_api_id,
          canvas_step_api_id as o_canvas_step_api_id
      FROM DATALAKE_SHARING.USERS_MESSAGES_EMAIL_OPEN_SHARED
      ),

      clicks as (
        select
          distinct email_address as click_address,
          message_variation_api_id as c_message_variation_api_id,
          canvas_step_api_id as c_canvas_step_api_id
      FROM DATALAKE_SHARING.USERS_MESSAGES_EMAIL_CLICK_SHARED
      ),

      campaign as (
        select
          id as campaign_id,
          name as campaign_name,
          time as updated_timestamp
      from DATALAKE_SHARING.CHANGELOGS_CAMPAIGN_SHARED
      ),

      canvas as (
        select
          id as canvas_id,
          name as canvas_name,
          time as updated_timestamp
      from DATALAKE_SHARING.CHANGELOGS_CANVAS_SHARED
      )

      SELECT deliveries.*, clicks.*, opens.*, campaign_name, canvas_name FROM deliveries
      LEFT JOIN opens
      ON (deliveries.delivered_address)=(opens.open_address)
      AND ((deliveries.d_message_variation_api_id)=(opens.o_message_variation_api_id) OR (deliveries.d_canvas_step_api_id)=(opens.o_canvas_step_api_id))
      LEFT JOIN clicks
      ON (deliveries.delivered_address)=(clicks.click_address)
      AND ((deliveries.d_message_variation_api_id)=(clicks.c_message_variation_api_id) OR (deliveries.d_canvas_step_api_id)=(clicks.c_canvas_step_api_id))
      LEFT JOIN campaign
        ON (deliveries.d_campaign_id)=(campaign.campaign_id)
      LEFT JOIN canvas
        ON (deliveries.d_canvas_id)=(canvas.canvas_id)
      qualify row_number() over (partition by deliveries.delivered_id ORDER BY campaign.updated_timestamp, canvas.updated_timestamp DESC) = 1
      ;;
  }

  dimension: campaign_name {
    description: "Campaign Name"
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  dimension: canvas_name {
    description: "Canvas nNme"
    type: string
    sql: ${TABLE}."CANVAS_NAME" ;;
  }

  dimension: canvas_step_id {
    description: "Canvas Step ID"
    type: string
    sql: ${TABLE}."D_CANVAS_STEP_API_ID" ;;
  }

  dimension: days_since_last_received {
    description: "Days between each email message delivered to an email address (null for a single send)"
    type: number
    sql: ${TABLE}."DIFF_DAYS" ;;
  }

  dimension: days_since_last_received_tier {
    description: "Tiered days between each email message delivered to an email address (null for a single send)"
    type: tier
    hidden: yes
    sql: COALESCE(${TABLE}."DIFF_DAYS",0) ;;
    tiers: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,28]
    style: integer
  }

  dimension: email_address {
    description: "Email address of the user"
    type: string
    sql: ${TABLE}."delivered_ADDRESS" ;;
  }

  dimension_group: first_delivered {
    description: "UTC epoch timestamp the first email was delivered to this user"
    label: "First Delivered (UTC)"
    type: time
    timeframes: [date, time]
    sql: ${TABLE}."FIRST_DELIVERED" ;;
  }

  dimension: message_variation_id {
    description: "Message variation ID if from a campaign"
    type: string
    sql: ${TABLE}."D_MESSAGE_VARIATION_API_ID" ;;
  }

  dimension_group: delivery {
    description: "UTC epoch timestamp the email was delivered"
    label: "Delivered Time (UTC)"
    type: time
    timeframes: [date, time, hour_of_day]
    sql: ${TABLE}."DELIVERED_TIMESTAMP" ;;
  }

  dimension: delivery_event {
    description: "Time-based ranking (1st, 2nd, 3rd, etc.) of message variations/canvas steps delivered to an email address"
    type: number
    sql: ${TABLE}."DELIVERY_EVENT" ;;
  }

  dimension: weeks_since_last_received {
    description: "Weeks between each email message delivered to an email address (null for a single send)"
    type: number
    sql: ${TABLE}."DIFF_WEEKS" ;;
  }

  dimension: weeks_since_last_received_tier {
    description: "Tiered weeks between each email message delivered to an email address (null for a single send)"
    type: tier
    hidden: yes
    sql: COALESCE(${TABLE}."DIFF_WEEKS",0) ;;
    tiers: [1,2,3,4,5,6,7,8,9,10,11,12,13,23,33,52]
    style: integer
  }

  measure: average_number_of_days_since_last_received {
    description: "Average amount of days between each email delivered to an email address (null for a single send)"
    type: average
    value_format_name: decimal_0
    sql: ${TABLE}."DIFF_DAYS";;
  }

  measure: count_distinct_email_address {
    description: "Count of unique email addresses"
    type: count_distinct
    sql: ${TABLE}."DELIVERED_ADDRESS" ;;
  }

  measure: emails_delivered {
    description: "Count of unique delivery ids"
    type: count_distinct
    sql: ${TABLE}."DELIVERED_ID" ;;
  }

  measure: unique_clicks {
    description: "Total unique clicks of campaigns/canvases per email address"
    type: number
    sql: count(distinct ${TABLE}."CLICK_ADDRESS", ${TABLE}."C_MESSAGE_VARIATION_API_ID")
      +count(distinct ${TABLE}."CLICK_ADDRESS", ${TABLE}."C_CANVAS_STEP_API_ID") ;;
  }

  measure: unique_opens {
    description: "Total unique opens of campaigns/canvases opened per email address"
    type: number
    sql: count(distinct ${TABLE}."OPEN_ADDRESS", ${TABLE}."O_MESSAGE_VARIATION_API_ID")
      +count(distinct ${TABLE}."OPEN_ADDRESS", ${TABLE}."O_CANVAS_STEP_API_ID") ;;
  }

  measure: unique_open_rate {
    description: "Email unique opens/deliveries"
    type: number
    value_format_name: percent_2
    sql: ${unique_opens}/${emails_delivered} ;;
  }

  measure: unique_click_rate {
    description: "Email unique opens/deliveries"
    type: number
    value_format_name: percent_2
    sql: ${unique_clicks}/${emails_delivered} ;;
  }
}
