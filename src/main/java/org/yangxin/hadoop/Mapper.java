package org.yangxin.hadoop;

/**
 * @author yangxin
 * 2023/6/25 19:51
 */
public interface Mapper {

    /**
     * @param line 读取到每一行数据
     * @param context 上下文/缓存
     */
    void map(String line, Context context);
}
