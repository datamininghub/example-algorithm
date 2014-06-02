package Generator

import au.com.bytecode.opencsv.CSVWriter
import java.io.{FileOutputStream, OutputStreamWriter}
import scopt.OptionParser
import scala.util.Random

case class Group(id: Long, name: String, items: List[Long] = List())

case class Dictionary(groups: List[Group] = List()) {
  def flat = groups
    .map(group => group.items.map((_, group.id, group.name)))
    .flatten

  def save(path: String) = {
    val writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(path)))
    for ((item_id, group_id, group_name) <- flat) {
      writer.writeNext(Array(item_id.toString, group_id.toString, group_name))
    }
    writer.close()
  }
}

object Dictionary {
  var last_item_id: Long = 0

  def next_item = {
    last_item_id += 1
    last_item_id
  }

  val rnd = new Random()

  def next_items(cfg: Config) =
    (0 to rnd.nextInt(cfg.max_items_in_group - cfg.min_items_in_group + 1)).toList.map(_ => next_item)

  def generate(cfg: Config) = {
    Dictionary((1 to cfg.groups).toList.map { id =>
      Group(id, f"Group $id%s", next_items(cfg))
    })
  }
}

case class Transaction(id: Long, user_id: Int, items: List[Long] = List()) {
  def flat =
    items.map((id, user_id, _))
}

object Transactions {

  var transaction_id: Long = 0

  def next_transaction(user_id: Int, items: List[Long]) = {
    transaction_id += 1
    Transaction(transaction_id, user_id, items)
  }

  val rnd = new Random()

  def next_items(flat_dictionary: List[(Long, Long, String)], cfg: Config) =
    (0 to rnd.nextInt(cfg.max_items_at_transaction - cfg.min_items_at_transaction + 1)).toList.map {
      _ => flat_dictionary(rnd.nextInt(flat_dictionary.size))._1
    }

  def generateAndSave(dict: Dictionary, path: String, cfg: Config) = {
    val writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(path)))
    val flat_dictionary = dict.flat
    (1 to cfg.users).toList.map {
      user_id =>
        (0 to rnd.nextInt(cfg.max_user_transactions - cfg.min_user_transactions + 1)).toList.map {
          id =>
            for ((transaction_id, transaction_user_id, item) <-
                 next_transaction(user_id, next_items(flat_dictionary, cfg)).flat) {
              writer.writeNext(Array(transaction_id.toString, transaction_user_id.toString, item.toString))
            }
            id
        }
    }
    writer.close()
  }
}

case class Config(transactions: String = Driver.default_transactions, dictionary: String = Driver.default_dictionary,
                  groups: Int = Driver.default_groups, min_items_in_group: Int = Driver.default_min_items_in_group, max_items_in_group: Int = Driver.default_max_items_in_group,
                  users: Int = Driver.default_users,
                  min_user_transactions: Int = Driver.default_min_user_transactions, max_user_transactions: Int = Driver.default_max_user_transactions,
                  min_items_at_transaction: Int = Driver.default_min_items_at_transaction, max_items_at_transaction: Int = Driver.default_max_items_at_transaction)

object Driver {
  val default_transactions = "transactions.csv"
  val default_dictionary = "dictionary.csv"

  val default_groups = 10
  val default_min_items_in_group = 1
  val default_max_items_in_group = 10

  val default_users = 50000
  val default_min_user_transactions = 3
  val default_max_user_transactions = 30
  val default_min_items_at_transaction = 1
  val default_max_items_at_transaction = 10

  val parser = new OptionParser[Config]("Generator.Driver") {
    head("DMH example algorithm data generator", "1.0")
    opt[String]("transactions") action {
      (transactions, config) => config.copy(transactions = transactions)
    } text f"transactions output file, default $default_transactions%s"
    opt[String]("dictionary") action {
      (dictionary, config) => config.copy(dictionary = dictionary)
    } text f"dictionary output file, default $default_dictionary%s"
    opt[Int]("groups") action {
      (groups, config) => config.copy(groups = groups)
    } text f"Groups available, default $default_groups%d"
    opt[Int]("min_items_in_group") action {
      (min_items_in_group, config) => config.copy(min_items_in_group = min_items_in_group)
    } text f"Each group contain no less of $default_min_items_in_group%d items"
    opt[Int]("max_items_in_group") action {
      (max_items_in_group, config) => config.copy(max_items_in_group = max_items_in_group)
    } text f"Each group contain no more of $default_max_items_in_group%d items"
    opt[Int]("users") action {
      (users, config) => config.copy(users = users)
    } text f"Users available, default $default_users%d"
    opt[Int]("min_user_transactions") action {
      (min_user_transactions, config) => config.copy(min_user_transactions = min_user_transactions)
    } text f"Each user have no less of $default_min_user_transactions%d transactions"
    opt[Int]("max_user_transactions") action {
      (max_user_transactions, config) => config.copy(max_user_transactions = max_user_transactions)
    } text f"Each user have no more of $default_max_user_transactions%d transactions"
    opt[Int]("min_items_at_transactions") action {
      (min_items_at_transaction, config) => config.copy(min_items_at_transaction = min_items_at_transaction)
    } text f"Each user transaction contains no less of $default_min_items_at_transaction%d items"
    opt[Int]("min_items_at_transactions") action {
      (max_items_at_transaction, config) => config.copy(max_items_at_transaction = max_items_at_transaction)
    } text f"Each user transaction contains no more of $default_max_items_at_transaction%d items"
  }

  def main(args: Array[String]): Unit =
    parser.parse(args, Config()) map {
      config =>
        val dictionary = Dictionary.generate(config)
        dictionary.save(config.dictionary)
        Transactions.generateAndSave(dictionary, config.transactions, config)
    }
}
