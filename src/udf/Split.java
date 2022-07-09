public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";

    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}