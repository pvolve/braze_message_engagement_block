# Email Messaging Frequency
view: email_messaging_frequency {
  derived_table: {
    sql: (SELECT date_trunc({% parameter date_granularity %}, to_timestamp(deliveries.time)) AS delivered_time,
        deliveries.email_address  AS delivered_address,
        deliveries.ID as delivered_id,
        count(distinct deliveries.id ) over (partition by delivered_time, delivered_address) AS frequency,
        row_number() over (partition by delivered_address, delivered_time order by delivered_time) as rank,
        opens.email_address as opened_address,
        opens.message_variation_api_id as opened_mv_id,
        opens.canvas_step_api_id as opened_cs_id,
        clicks.email_address as clicked_address,
        clicks.message_variation_api_id as clicked_mv_id,
        clicks.canvas_step_api_id as clicked_cs_id,
        campaign.name as campaign_name,
        canvas.name as canvas_name
        FROM DATALAKE_SHARING.USERS_MESSAGES_EMAIL_DELIVERY_SHARED  AS deliveries
        LEFT JOIN DATALAKE_SHARING.USERS_MESSAGES_EMAIL_OPEN_SHARED  AS opens ON (deliveries.email_address)=(opens.email_address)
                    AND
                    ((deliveries.message_variation_api_id)=(opens.message_variation_api_id)
                    OR
                    (deliveries.canvas_step_api_id)=(opens.canvas_step_api_id))
        LEFT JOIN DATALAKE_SHARING.USERS_MESSAGES_EMAIL_CLICK_SHARED  AS clicks ON (deliveries.email_address)=(clicks.email_address)
                    AND
                    ((deliveries.message_variation_api_id)=(clicks.message_variation_api_id)
                    OR
                    (deliveries.canvas_step_api_id)=(clicks.canvas_step_api_id))
        LEFT JOIN DATALAKE_SHARING.CHANGELOGS_CAMPAIGN_SHARED AS campaign ON deliveries.campaign_id = campaign.id
          and deliveries.time >= campaign.time
        LEFT JOIN DATALAKE_SHARING.CHANGELOGS_CANVAS_SHARED as canvas on deliveries.canvas_id = canvas.id
          and deliveries.time >= canvas.time
      WHERE
      {% condition campaign_name %} campaign_name {% endcondition %}
      AND
      {% condition canvas_name %} canvas_name {% endcondition %}
      AND
      {% condition message_variation_id %} deliveries.message_variation_api_id {% endcondition %}
      qualify row_number() over (partition by delivered_id ORDER BY  canvas.time, campaign.time DESC) = 1)
      ;;
  }

  filter: campaign_name {
    description: "Campaign name"
    suggest_explore: users_messages_email_send
    suggest_dimension: campaign_name
  }

  filter: canvas_name {
    description: "Canvas name"
    suggest_explore: users_messages_email_send
    suggest_dimension: canvas_name
  }

  # filter: canvas_step_id {
  #   description: "canvas step id if from a canvas"
  #   suggest_explore: users_messages_email_send
  #   suggest_dimension: canvas_step_id
  # }

  filter: message_variation_id {
    description: "Message variation id if from a campaign"
    suggest_explore: users_messages_email_send
    suggest_dimension: message_variation_id
  }

  parameter: date_granularity {
    description: "Specify daily, weekly or monthly marketing pressure"
    type: string
    default_value: "day"
    allowed_value: {
      value: "day"
    }
    allowed_value: {
      value: "week"
    }
    allowed_value: {
      value: "month"
    }
  }

  dimension_group: delivered_time {
    description: "Time email was delivered (UTC)"
    label: "Delivered Time (UTC)"
    type: time
    timeframes: [hour_of_day,
      date,
      day_of_week,
      week, month,
      quarter,
      year]
    sql: ${TABLE}."DELIVERED_TIME" ;;
  }

  dimension: email_address {
    description: "Email address of the user"
    type: string
    sql: ${TABLE}."DELIVERED_ADDRESS" ;;
  }

  dimension: frequency {
    description: "Number of emails sent per (day/week/month)"
    type: number
    sql: ${TABLE}."FREQUENCY" ;;
  }

  measure: emails_delivered {
    description: "Unique email delivery events"
    type: sum
    sql: CASE WHEN rank=1 then ${frequency} else null end ;;
  }

  measure: unique_opens_mvid {
    description: "Unique opens corresponding to message variations"
    type: count_distinct
    hidden: yes
    sql: ${TABLE}."OPENED_ADDRESS", ${TABLE}."OPENED_MV_ID" ;;
  }

  measure: unique_opens_csid {
    description: "Unique opens corresponding to canvas steps"
    type: count_distinct
    hidden: yes
    sql: ${TABLE}."OPENED_ADDRESS", ${TABLE}."OPENED_CS_ID" ;;
  }

  measure: unique_clicks_mvid {
    description: "Unique clicks corresponding to message variations"
    type: count_distinct
    hidden: yes
    sql: ${TABLE}."CLICKED_ADDRESS", ${TABLE}."CLICKED_MV_ID" ;;
  }

  measure: unique_clicks_csid {
    description: "Unique clicks corresponding to canvas steps"
    type: count_distinct
    hidden: yes
    sql: ${TABLE}."CLICKED_ADDRESS", ${TABLE}."CLICKED_CS_ID" ;;
  }

  measure: unique_opens {
    description: "Times a recipient opened an email campaign or canvas (does not count the same person opening the same campaign or canvas more than once)"
    type: number
    sql: COALESCE(${unique_opens_mvid},0)+COALESCE(${unique_opens_csid},0);;
  }

  measure: unique_clicks {
    description: "Times a recipient opened an email campaign or canvas (does not count the same person opening the same campaign or canvas more than once)"
    type: number
    sql: COALESCE(${unique_clicks_mvid},0)+COALESCE(${unique_clicks_csid},0) ;;
  }

  measure: delivery_occasions {
    description: "Occasions certain frequency of emails was sent to a user by date granularity"
    type: number
    sql: COUNT(CASE WHEN rank=1 then ${frequency} else null end) ;;
  }

  measure: unique_click_rate {
    description: "Unique clicks/emails delivered"
    type: number
    value_format_name: percent_2
    sql: ${unique_clicks}/NULLIF(${emails_delivered},0) ;;
  }

  measure: unique_open_rate {
    description: "Unique opens/emails delivered"
    type: number
    value_format_name: percent_2
    sql: ${unique_opens}/NULLIF(${emails_delivered},0) ;;
  }

  measure: unique_recipients {
    description: "Unique email addresses that received an email campaign"
    type: count_distinct
    sql: ${TABLE}."DELIVERED_ADDRESS" ;;
  }
}
