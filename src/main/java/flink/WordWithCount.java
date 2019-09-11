package flink;

/**
 * @ Author     ：zsp
 * @ Date       ：Created in 11:14 2019/9/11
 * @ Description：
 * @ Modified By：
 * @ Version:
 */
public class WordWithCount
{
    public String word;
    public long count;

    public WordWithCount()
    {
    }

    public WordWithCount(String word, long count)
    {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString()
    {
        return "WordWithCount{" +
               "word='" + word + '\'' +
               ", count=" + count +
               '}';
    }
}
