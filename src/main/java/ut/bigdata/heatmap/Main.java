package ut.bigdata.heatmap;

import org.apache.flink.api.java.utils.ParameterTool;
import ut.bigdata.heatmap.config.Config;
import ut.bigdata.heatmap.config.Parameters;

import static ut.bigdata.heatmap.config.Parameters.STRING_PARAMS;
import static ut.bigdata.heatmap.config.Parameters.INT_PARAMS;
import static ut.bigdata.heatmap.config.Parameters.BOOL_PARAMS;

public class Main {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        Parameters inputParams = new Parameters(tool);
        Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
    }
}
