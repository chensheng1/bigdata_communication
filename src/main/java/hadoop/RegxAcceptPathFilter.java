package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by asus on 2020/7/5.
 */
//过滤掉zip以外的文件
public class RegxAcceptPathFilter implements PathFilter {
    private  final String regex;
    public RegxAcceptPathFilter(String regex) {
        this.regex=regex;
    }
    @Override
    public boolean accept(Path path) {

        boolean flag=path.toString().matches(regex);
        return flag;
    }
}