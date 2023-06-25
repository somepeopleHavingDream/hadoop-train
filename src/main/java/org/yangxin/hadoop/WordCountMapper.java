package org.yangxin.hadoop;

import java.util.Objects;

/**
 * 自定义wc实现类
 *
 * @author yangxin
 * 2023/6/25 19:53
 */
public class WordCountMapper implements Mapper {

    @Override
    public void map(String line, Context context) {
        String[] wordArray = line.split("\t");
        for (String word : wordArray) {
            Object value = context.get(word);
            if (Objects.isNull(value)) {
                // 表示没有出现该单词
                context.write(word, 1);
            } else {
                // 取出单词对应的次数+1
                int v = Integer.parseInt(value.toString());
                context.write(word, v + 1);
            }
        }
    }
}
