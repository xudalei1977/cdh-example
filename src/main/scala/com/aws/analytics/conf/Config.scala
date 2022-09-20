package com.aws.analytics.conf

case class Config(
                   env: String = "",
                   kuduMaster: String = "",
                   tableName: String = ""
                 )


object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config]("spark ss "+programName) {
      head(programName, "1.0")
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x)).text("env: dev or prod")

      programName match {
        case "KuduSample" =>
          opt[String]('k', "kuduMaster").optional().action((x, config) => config.copy(kuduMaster = x)).text("kudu master and port")
          opt[String]('t', "tableName").optional().action((x, config) => config.copy(tableName = x)).text("kudu table name")
      }

    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        System.exit(-1)
        null
      }
    }

  }

}

