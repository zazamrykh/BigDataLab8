from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local", "WordCount")

    # Входные данные
    text_file = sc.textFile("input.txt")

    # WordCount
    counts = (text_file.flatMap(lambda line: line.split(" "))
                         .map(lambda word: (word, 1))
                         .reduceByKey(lambda a, b: a + b))

    # Сохранение результата в директорию "output"
    counts.saveAsTextFile("output")

    sc.stop()
