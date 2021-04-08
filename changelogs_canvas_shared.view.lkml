view: changelogs_canvas_shared {
  sql_table_name: "DATALAKE_SHARING"."CHANGELOGS_CANVAS_SHARED"
    ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}."ID" ;;
  }

  dimension: api_id {
    type: string
    sql: ${TABLE}."API_ID" ;;
  }

  dimension: app_group_id {
    type: string
    sql: ${TABLE}."APP_GROUP_ID" ;;
  }

  dimension: conversion_behaviors {
    type: string
    sql: ${TABLE}."CONVERSION_BEHAVIORS" ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}."NAME" ;;
  }

  dimension: time {
    type: number
    sql: ${TABLE}."TIME" ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name]
  }
}
