package com.forsrc.spark.cdap.spark;


import co.cask.cdap.api.workflow.AbstractWorkflow;

public class SparkWordCountWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setDescription("Runs SparkWordCount program");
        addSpark(SparkWordCountSpark.class.getSimpleName());
        // addMapReduce(.class.getSimpleName());
    }
}