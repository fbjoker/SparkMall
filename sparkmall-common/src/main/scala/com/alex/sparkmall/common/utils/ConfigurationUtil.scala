package com.alex.sparkmall.common.utils

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

object ConfigurationUtil{
  def apply(propertiesName:String) = {
    val configurationUtil = new ConfigurationUtil()
    if (configurationUtil.config == null) {
      configurationUtil.config = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
        .configure(new Parameters().properties().setFileName(propertiesName)).getConfiguration
    }
    configurationUtil
  }

}
class ConfigurationUtil(){
  var config:FileBasedConfiguration=null

}