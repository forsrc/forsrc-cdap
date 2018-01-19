package com.forsrc.spark.cdap.spark;

import org.apache.spark.SparkConf;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.AbstractSpark;

public class SparkWordCountSpark extends AbstractSpark {

    @Override
    public void configure() {
      setName(SparkWordCountSpark.class.getSimpleName());
      setDescription("Spark kWordCount Program");
      setMainClass(SparkWordCountJavaSparkMain.class);
      setDriverResources(new Resources(1024));
      setExecutorResources(new Resources(1024));
    }

    @Override
    public void initialize() throws Exception {
        getContext().setSparkConf(new SparkConf().set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=256m"));
    }

  }
