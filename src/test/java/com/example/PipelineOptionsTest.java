package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PipelineOptionsTest {
    @Test
    public void testAs() {
        MyPipelineOptions myOptions = PipelineOptionsFactory.create().as(MyPipelineOptions.class);
        myOptions.setInputFile("anotherinput.txt");
        ApplicationNameOptions anOptions = myOptions.as(ApplicationNameOptions.class);
        anOptions.setAppName("PipelineOptionsTest");

        MyPipelineOptions myOptions2 = anOptions.as(MyPipelineOptions.class);
        Assert.assertEquals("as()を介して別の型にキャストしてまた戻した時、元のデータは維持される",
            "anotherinput.txt", myOptions2.getInputFile());

        myOptions2.setOutputFile("anotheroutput.txt");
        Assert.assertEquals("ある参照を通じてオプションを変更すると別の参照にも反映される",
            "anotheroutput.txt", myOptions.getOutputFile());
    }

    @Test
    public void setAndGetFromPipeline() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.getOptions().as(MyPipelineOptions.class).setInputFile("anotherinput.txt");
        Assert.assertEquals("Pipeline.getOptions() を通して set した結果が get できる",
            "anotherinput.txt", p.getOptions().as(MyPipelineOptions.class).getInputFile());
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Default.String("input.txt")
        String getInputFile();
        void setInputFile(String value);

        @Default.String("output.txt")
        String getOutputFile();
        void setOutputFile(String value);
    }
}
