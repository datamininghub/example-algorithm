package Algorithm

import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, MultipleInputs, FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.conf.{Configured, Configuration}
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import java.io._
import au.com.bytecode.opencsv.{CSVWriter, CSVReader}

class Record extends Writable {
  val group_id = new LongWritable()
  val item = new LongWritable()
  val user_id = new LongWritable()

  override def write(out: DataOutput) = {
    group_id.write(out)
    item.write(out)
    user_id.write(out)
  }

  override def readFields(in: DataInput) = {
    group_id.readFields(in)
    item.readFields(in)
    user_id.readFields(in)
  }

  override def toString: String = {
    "group_id = " + group_id + "; item = " + item + "; user_id = " + user_id
  }
}

object Joiner {

  object Type extends Enumeration {
    val Left, Right = Value
  }

  class TypeWritable(tp: Type.Value) extends WritableComparable[TypeWritable] {
    var data: Type.Value = tp

    override def compareTo(o: TypeWritable) =
      data.compare(o.data)

    override def write(out: DataOutput) =
      WritableUtils.writeString(out, data.toString)

    override def readFields(in: DataInput) =
      data = Type.withName(WritableUtils.readString(in))
  }

  case class Value(tw: TypeWritable, key: Text) extends WritableComparable[Value] {

    def this() =
      this(new TypeWritable(Joiner.Type.Left), new Text())

    def this(tp: Type.Value) =
      this(new TypeWritable(tp), new Text())

    override def write(out: DataOutput) = {
      tw.write(out)
      key.write(out)
    }

    override def readFields(in: DataInput) = {
      tw.readFields(in)
      key.readFields(in)
    }

    override def compareTo(o: Value) =
      key.compareTo(o.key)

    override def hashCode() = key.hashCode()
  }

  class Comparator extends WritableComparator(classOf[Value], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]) =
      (a.asInstanceOf[Value], b.asInstanceOf[Value]) match {
        case (kv1, kv2) =>
          kv1.compareTo(kv2) match {
            case 0 => kv2.tw.compareTo(kv1.tw)
            case res => res
          }
      }
  }

  class Grouping extends WritableComparator(classOf[Value], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]) =
      (a.asInstanceOf[Value], b.asInstanceOf[Value]) match {
        case (kv1, kv2) => kv1.compareTo(kv2)
      }
  }

}

object Enrich {

  class LeftMap extends Mapper[LongWritable, Text, Joiner.Value, Text] {
    val join_key = new Joiner.Value(Joiner.Type.Left)

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Joiner.Value, Text]#Context) = {
      val line = new CSVReader(new StringReader(value.toString)).readNext()
      if (line != null && line.size == 3) {
        join_key.key.set(line(1))
        context.write(join_key, value)
      }
    }
  }

  class RightMap extends Mapper[LongWritable, Text, Joiner.Value, Text] {
    val join_key = new Joiner.Value(Joiner.Type.Right)

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Joiner.Value, Text]#Context) = {
      val line = new CSVReader(new StringReader(value.toString)).readNext()
      if (line != null && line.size == 3) {
        join_key.key.set(line(0))
        context.write(join_key, value)
      }
    }
  }

  class Reduce extends Reducer[Joiner.Value, Text, LongWritable, Record] {
    val record = new Record()
    val transaction = new LongWritable()

    override def reduce(key: Joiner.Value, values: java.lang.Iterable[Text], context: Reducer[Joiner.Value, Text, LongWritable, Record]#Context) = {
      val it = values.iterator()

      if (it.hasNext) {
        val line = new CSVReader(new StringReader(it.next().toString)).readNext()
        record.group_id.set(line(1).toLong)
        while (it.hasNext) {
          val line = new CSVReader(new StringReader(it.next().toString)).readNext()
          transaction.set(line(0).toLong)
          record.user_id.set(line(1).toLong)
          record.item.set(line(2).toLong)
          context.write(transaction, record)
        }
      }
    }
  }

  def run(cfg: Configuration, transactions: Path, dictionary: Path, output: Path, force: Boolean, verbose: Boolean) = {
    val job: Job = new Job(cfg, "Enrich transaction: " + transactions + " use dictionary: " + dictionary)
    job setJarByClass getClass

    MultipleInputs addInputPath(job, transactions, classOf[TextInputFormat], classOf[LeftMap])
    MultipleInputs addInputPath(job, dictionary, classOf[TextInputFormat], classOf[RightMap])

    job setOutputFormatClass classOf[SequenceFileOutputFormat[LongWritable, Record]]

    FileInputFormat addInputPath(job, transactions)
    FileInputFormat addInputPath(job, dictionary)

    FileOutputFormat setOutputPath(job, output)

    job setMapOutputKeyClass classOf[Joiner.Value]
    job setMapOutputValueClass classOf[Text]

    job setOutputKeyClass classOf[LongWritable]
    job setOutputValueClass classOf[Record]

    job setReducerClass classOf[Reduce]

    job setSortComparatorClass classOf[Joiner.Comparator]
    job setGroupingComparatorClass classOf[Joiner.Grouping]

    if (force)
      output.getFileSystem(cfg).delete(output, true)

    job waitForCompletion verbose
  }

}

object Algorithm {

  class Map extends Mapper[LongWritable, Record, LongWritable, LongWritable] {
    override def map(key: LongWritable, value: Record, context: Mapper[LongWritable, Record, LongWritable, LongWritable]#Context) = {
      context.write(value.user_id, value.item)
    }
  }

  class Reduce extends Reducer[LongWritable, LongWritable, Text, NullWritable] {
    val line = new Text()

    var max_items_per_transaction: Int = Driver.default_max_items_per_transaction

    override def setup(context: Reducer[LongWritable, LongWritable, Text, NullWritable]#Context) = {
      super.setup(context)

      max_items_per_transaction =
        context.getConfiguration.getInt("max_items_per_transaction", Driver.default_max_items_per_transaction)
    }

    override def reduce(key: LongWritable, values: java.lang.Iterable[LongWritable], context: Reducer[LongWritable, LongWritable, Text, NullWritable]#Context) = {
      val items = scala.collection.mutable.Map[Long, Long]()
      val it = values.iterator()

      while (it.hasNext) {
        val item = it.next.get()
        items.put(item, items.get(item).getOrElse(0L) + 1L)
      }

      if (!items.isEmpty) {
        val bests =
          if (items.size <= max_items_per_transaction) items.toSeq
          else items.toSeq.sortBy(_._2).dropRight(items.size - max_items_per_transaction)

        for (best <- bests) {
          val writer = new StringWriter()
          val csv_writer = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "")
          val new_line = List(key.get.toString, best._1.toString)
          csv_writer.writeNext(new_line.toArray)
          context.write(new Text(writer.toString), NullWritable.get())
        }
      }
    }
  }


  def run(cfg: Configuration, input: Path, output: Path, max: Int, force: Boolean, verbose: Boolean) = {
    val job: Job = new Job(cfg, "Example algorithm over: " + input)
    job setJarByClass getClass

    job setInputFormatClass classOf[SequenceFileInputFormat[LongWritable, Record]]
    job setOutputFormatClass classOf[TextOutputFormat[Text, NullWritable]]

    FileInputFormat addInputPath(job, input)
    FileOutputFormat setOutputPath(job, output)

    job setMapOutputKeyClass classOf[LongWritable]
    job setMapOutputValueClass classOf[LongWritable]

    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[NullWritable]

    job setMapperClass classOf[Map]
    job setReducerClass classOf[Reduce]

    job.getConfiguration.setInt("max_items_per_transaction", max)

    if (force)
      output.getFileSystem(cfg).delete(output, true)

    job waitForCompletion verbose
  }

}

object Driver extends Configured with Tool {

  val default_transactions = "transactions.csv"
  val default_dictionary = "dictionary.csv"

  val default_output = "output"
  val default_temp = "temp"

  val default_max_items_per_transaction = 10

  case class Config(transactions: Path = new Path(default_transactions), dictionary: Path = new Path(default_dictionary),
                    max_items_per_transaction: Int = default_max_items_per_transaction,
                    output: Path = new Path(default_output), temp: Path = new Path(default_temp),
                    debug: Boolean = false, force: Boolean = false)

  val parser = new scopt.OptionParser[Config]("example-algorithm.jar") {
    head("DMH example algorithm", "1.0")
    opt[String]("transactions") action {
      (transactions, config) => config.copy(transactions = new Path(transactions))
    } text f"transactions folder, default: $default_transactions%s"
    opt[String]("dictionary") action {
      (dictionary, config) => config.copy(dictionary = new Path(dictionary))
    } text f"dictionary folder, default: $default_dictionary%s"
    opt[Int]("max") action {
      (count, config) => config.copy(max_items_per_transaction = count)
    } text f"Each predict transaction contain no more of <value> items, default <value> is $default_max_items_per_transaction"
    opt[String]('o', "output") action {
      (output, config) => config.copy(output = new Path(output))
    } text f"output folder, default: $default_output%s"
    opt[String]("temp") action {
      (temp, config) => config.copy(temp = new Path(temp))
    } text f"temp folder, default: $default_temp%s"
    opt[Unit]('d', "debug") action {
      (_, config) => config.copy(debug = true)
    }
    opt[Unit]('f', "force") action {
      (_, config) => config.copy(force = true)
    } text "force remove output when exists"
  }

  def run(args: Array[String]) = {
    parser.parse(args, Config()).fold(-1)({
      config =>
        if (!Enrich.run(getConf, config.transactions, config.dictionary, config.temp, config.force, config.debug)) -2
        else {
          if (!Algorithm.run(getConf, config.temp, config.output, config.max_items_per_transaction, config.force, config.debug)) -3
          else 0
        }
    })
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(new Configuration(), this, args))
  }

}
