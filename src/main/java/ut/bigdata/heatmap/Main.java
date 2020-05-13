package ut.bigdata.heatmap;

import org.apache.flink.api.java.utils.ParameterTool;
import ut.bigdata.config.Config;
import ut.bigdata.config.Parameters;

import static ut.bigdata.config.Parameters.STRING_PARAMS;
import static ut.bigdata.config.Parameters.INT_PARAMS;
import static ut.bigdata.config.Parameters.BOOL_PARAMS;

public class Main {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        Parameters inputParams = new Parameters(tool);
        Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);
    }
}
