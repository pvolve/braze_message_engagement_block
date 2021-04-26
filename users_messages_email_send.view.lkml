# Email Send Events
view: users_messages_email_send {
  derived_table: {
    sql:
      with sends as (
        select * from DATALAKE_SHARING.USERS_MESSAGES_EMAIL_SEND_SHARED
      ),
      campaign as (
        select id as campaign_id,
        name as campaign_name,
        time as campaign_updated_timestamp
      from DATALAKE_SHARING.CHANGELOGS_CAMPAIGN_SHARED
      ),
      canvas as (
        select id as canvas_id,
        name as canvas_name,
        time as canvas_updated_timestamp
      from DATALAKE_SHARING.CHANGELOGS_CANVAS_SHARED
      ),
      joined as (
        select sends.*, campaign_name, canvas_name
        FROM sends
        LEFT JOIN campaign
          ON sends.campaign_id = campaign.campaign_id
        LEFT JOIN canvas
          ON sends.canvas_id = canvas.canvas_id
        qualify row_number() over (partition by sends.id ORDER BY campaign_updated_timestamp, canvas_updated_timestamp DESC) = 1
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

  dimension: campaign_id {
    description: "id of the campaign if from a campaign"
    type: string
    sql: ${TABLE}."CAMPAIGN_ID" ;;
  }

  dimension: campaign_name {
    description: "name of the campaign"
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  dimension: canvas_id {
    description: "id of the canvas if from a canvas"
    type: string
    sql: ${TABLE}."CANVAS_ID" ;;
  }

  dimension: canvas_name {
    description: "name of the canvas"
    type: string
    sql: ${TABLE}."CANVAS_NAME" ;;
  }

  dimension: canvas_step_id {
    description: "id of the step for this message if from a Canvas"
    type: string
    sql: ${TABLE}."CANVAS_STEP_API_ID" ;;
  }

  dimension: canvas_variation_id {
    description: "id of the Canvas variation the user is in if from a Canvas"
    type: string
    sql: ${TABLE}."CANVAS_VARIATION_API_ID" ;;
  }

  dimension: email_address {
    description: "email address for this event"
    type: string
    sql: ${TABLE}."EMAIL_ADDRESS" ;;
  }

  dimension_group: email_send_time {
    description: "timestamp of the email send"
    type: time
    datatype: epoch
    timeframes: [
      raw,
      time,
      date,
      day_of_week,
      hour_of_day,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}."TIME" ;;
  }

  dimension: email_send_timezone {
    description: "IANA timezone of the user at the time of the event"
    hidden: yes
    type: string
    sql: ${TABLE}."TIMEZONE" ;;
  }

  dimension: external_user_id {
    description: "External ID of the user"
    type: string
    sql: ${TABLE}."EXTERNAL_USER_ID" ;;
  }

  dimension: message_variation_id {
    description: "id of the message variation if from a campaign"
    type: string
    sql: ${TABLE}."MESSAGE_VARIATION_API_ID" ;;
  }

  dimension: send_id {
    description: "id of the message if specified for the campaign (See Send Identifier under REST API Parameter Definitions)"
    hidden: yes
    type: string
    sql: ${TABLE}."SEND_ID" ;;
  }

  dimension: user_id {
    description: "Braze id of the user"
    type: string
    sql: ${TABLE}."USER_ID" ;;
  }

  measure: emails_sent {
    description: "distinct count of email send event IDs"
    type: count_distinct
    sql: ${TABLE}."ID" ;;
  }
}
