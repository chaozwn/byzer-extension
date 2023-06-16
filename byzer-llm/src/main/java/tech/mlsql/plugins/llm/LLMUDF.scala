package tech.mlsql.plugins.llm

import org.apache.spark.sql.UDFRegistration
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * 6/16/23 WilliamZhu(allwefantasy@gmail.com)
 */
object LLMUDF {
  def llm_stack(uDFRegistration: UDFRegistration) = {
    // llm_stack(chat(array(...)))
    uDFRegistration.register("llm_stack", (calls: Seq[String], newParams: Seq[String]) => {

      val obj = JSONTool.jParseJsonArray(calls.head).getJSONObject(0)
      val predict = obj.getString("predict")
      val input = obj.getJSONObject("input")
      val instruction = input.getString("instruction")
      
      val his_instruction = s"${instruction}${predict}"
      
      val query = JSONTool.jParseJsonObj(newParams.head)

      query.put("instruction", s"${his_instruction}\n${query.getString("instruction")}")

      Seq(query.toString)
    })
  }

  def llm_param(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_param", (input: Map[String, String]) => {

      val instruction = input("instruction")
      val system_msg = input.getOrElse("system_msg","")
      // system_role,user_role,assistant_role
      var systemRole = input.getOrElse("system_role", "")
      var userRole = input.getOrElse("user_role", "")
      var assistantRole = input.getOrElse("assistant_role", "")

      if (systemRole != "") {
        systemRole = s"${systemRole}:"
      }

      if (userRole != "") {
        userRole = s"${userRole}:"
      }

      if (assistantRole != "") {
        assistantRole = s"${assistantRole}:"
      }

      val newMap = input ++ Map("instruction"-> s"${systemRole}\n${system_msg}${userRole}${instruction}\n${assistantRole}")

      Seq(JSONTool.toJsonStr(newMap))
    })
  }

  def llm_response_full(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_response_full", (calls: Seq[String]) => {
      val obj = JSONTool.jParseJsonArray(calls.head).getJSONObject(0)
      val predict = obj.getString("predict")
      val input = obj.getJSONObject("input")
      val instruction = input.getString("instruction")
      s"${instruction}${predict}"
    })
  }

  def llm_response_predict(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("llm_response_predict", (calls: Seq[String]) => {
      val obj = JSONTool.jParseJsonArray(calls.head).getJSONObject(0)
      obj.getString("predict")
    })
  }
}
