package com.skt.tcore.model

import java.sql.Timestamp

case class Alarm(alarmType: String, ruleId: String, detect: Boolean, occurCount: Int, occurTimestamp: Timestamp, payload: String)
