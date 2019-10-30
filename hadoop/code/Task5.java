public class Task5 {
    public static void main(String[] args) throws Exception{
        String[] argInit = {args[0], args[1] + "/iter0"};
        InitLPA.main(argInit);
        String[] argLPA = {args[1] + "/iter", args[1] + "/iter"};
        LabelPropagation.main(argLPA);
        String[] argCollection = {args[1] + "/iter20", args[1] + "/collection"};
        LabelCollection.main(argCollection);
    }
}
