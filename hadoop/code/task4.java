public class task4 {
    public static void main(String[] args) {
        if(args.length != 4) {  // 输入参数个数为4个
            System.err.println("Usage: task4 <in> <out>");
            System.exit(4);
        }
        if(args[2].charAt(args[2].length()-1) == '/') { // 若模块二输出路径最后以/结尾
            args[2] = args[2].substring(0, args[2].length()-1);
        }
        String[] arg1 = {args[0], args[1]}; // 模块一的输入参数
        String[] arg2 = {args[1], args[2]}; // 模块二的输入参数
        String[] arg3 = {args[2]+"14", args[3]};    // 模块三的输入参数
        try {
            GraghBuilder.main(arg1);    // 逐一传入三个模块
            PageRank.main(arg2);
            GetResult.main(arg3);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
