

import Main.orders

import java.io.{File, PrintWriter}
import java.sql.{DriverManager, SQLException}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object Main extends App {
  // Order case class representing orders data
  case class Order(
                    timestamp: String,
                    productName: String,
                    expiryDate: LocalDate,
                    quantity: Int,
                    unitPrice: BigDecimal,
                    channel: String,
                    paymentMethod: String,
                    discount: BigDecimal = 0
                  ) {

    // Calculate the original price of the order based on quantity of units and their price per unit
    def originalPrice: BigDecimal = quantity * unitPrice

    // Calculate the final price after applying the discount
    def finalPrice: BigDecimal = {
      val discountAmount = originalPrice * (discount / 100)
      originalPrice - discountAmount
    }
  }


  // Object containing various QUALIFYING RULES and CALCULATION RULES for discounts
  object DiscountRules {
    //parsing dates from the timestamp
    val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    // Calculate the days until expiry for orders
    def daysUntilExpiry(order: Order): Long = {
      val transactionDate = LocalDate.parse(order.timestamp.take(10), dateFormatter)
      ChronoUnit.DAYS.between(transactionDate, order.expiryDate)
    }

    // Expiry-based discount rule: 1% discount for each day under 30 days to expiry
    def expiryBasedDiscount(order: Order): BigDecimal = {
      val daysRemaining = daysUntilExpiry(order)
      if (daysRemaining <= 29 && daysRemaining >= 1) {
        30 - daysRemaining
      } else {
        0
      }
    }

    // Discount for cheese and wine products as on sale
    def cheeseWineDiscount(order: Order): BigDecimal = {
      if (order.productName.toLowerCase.contains("cheese")) {
        10
      } else if (order.productName.toLowerCase.contains("wine")) {
        5
      } else {
        0
      }
    }

    // Special discount for transactions on the 23rd of March
    def specialDiscount(order: Order): BigDecimal = {
      val transactionDate = LocalDate.parse(order.timestamp.take(10), dateFormatter)
      if (transactionDate.getMonthValue == 3 && transactionDate.getDayOfMonth == 23) {
        50
      } else {
        0
      }
    }

    // Discount based on the quantity of units bought of the same product
    def quantityBasedDiscount(order: Order): BigDecimal = {
      order.quantity match {
        case q if q >= 6 && q <= 9 => 5
        case q if q >= 10 && q <= 14 => 7
        case q if q > 15 => 10
        case _ => 0
      }
    }

    // Special discount for sales through the App based on quantity rounded to nearest multiple of 5
    def appBasedDiscount(order: Order): BigDecimal = {
      if (order.channel.toLowerCase == "app") {
        val roundedQuantity = ((order.quantity + 4) / 5) * 5
        (roundedQuantity / 5) * 5 // Each multiple of 5 adds 5% to the discount
      } else {
        0
      }
    }

    // Discount for sales using Visa as a promotion for using cards instead of cash
    def visaCardDiscount(order: Order): BigDecimal = {
      if (order.paymentMethod.toLowerCase == "visa") {
        5
      } else {
        0
      }
    }

  }

  // Object to apply discount rules to orders
  object DiscountProcessor {
    // Function to apply discount rules to each order and return an updated one with a discount
    def getOrderWithDiscount(
                              order: Order,
                              rules: List[(Order => Boolean, Order => BigDecimal)]
                            ): Order = {
      val applicableDiscounts = rules
        .map { case (_, discountFunction) => discountFunction(order) }
        .sorted(Ordering[BigDecimal].reverse)
        .take(2)
        .sum / 2 // Average the top two discounts

      order.copy(discount = applicableDiscounts)
    }

  }

  // Object to read orders from a CSV file
  object OrderReader {
    // Function to read and parse orders from a CSV file
    def readOrdersFromCSV(filePath: String): List[Order] = {
      val source = Try(Source.fromFile(filePath))
      source match {
        case Success(s) =>
          val orders = s.getLines().drop(1).map { line =>
            val Array(timestamp, productName, expiryDateStr, quantityStr, unitPriceStr, channel, paymentMethod) = line.split(",")
            val quantity = quantityStr.trim.toInt
            val unitPrice = BigDecimal(unitPriceStr.trim)
            val expiryDate = LocalDate.parse(expiryDateStr.trim, DateTimeFormatter.ISO_LOCAL_DATE)
            Order(timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod)
          }.toList
          s.close()
          orders
        case Failure(e) =>
          SimpleLogger.error(s"Error reading file: ${e.getMessage}")
          List.empty[Order]
      }
    }
  }


  object Orderwriter {
    def writeOrdersToDatabase(connection: java.sql.Connection, orders: List[Order]): Unit = {
      val insertStatement =
        """
          |INSERT INTO orders (order_date, expiry_date, product_name, quantity, unit_price,
          |                   channel, payment_method, discount, original_price, final_price)
          |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |""".stripMargin

      val preparedStatementTry = Try(connection.prepareStatement(insertStatement))

      preparedStatementTry match {
        case Success(ps) =>
          val successCount = orders.foldLeft(0) { (count, order) =>
            // Set parameters and execute the insertion, returning the updated count
            Try {
              ps.setDate(1, java.sql.Date.valueOf(order.timestamp.take(10)))
              ps.setDate(2, java.sql.Date.valueOf(order.expiryDate))
              ps.setString(3, order.productName)
              ps.setInt(4, order.quantity)
              ps.setBigDecimal(5, order.unitPrice.bigDecimal)
              ps.setString(6, order.channel)
              ps.setString(7, order.paymentMethod)
              ps.setBigDecimal(8, order.discount.bigDecimal)
              ps.setBigDecimal(9, order.originalPrice.bigDecimal)
              ps.setBigDecimal(10, order.finalPrice.bigDecimal)

              ps.executeUpdate()
            } match {
              case Success(_) => count + 1 // Increment the count if successful
              case Failure(e) =>
                SimpleLogger.error(s"Failed to insert order: ${e.getMessage}")
                count
            }
          }

          SimpleLogger.info(s"Successfully inserted $successCount orders into the database.")

          ps.close()

        case Failure(e) =>
          SimpleLogger.error(s"Could not prepare statement: ${e.getMessage}")
      }
    }
  }


  object SimpleLogger {

    val logFilePath = "src\\log\\rules_engine.log"
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def log(level: String, message: String): Unit = {
      val timestamp = LocalDateTime.now().format(dateFormatter)
      val logMessage = s"$timestamp $level $message"

      // Write the log message to the file (in append mode)
      val writer = new PrintWriter(new java.io.FileWriter(new File(logFilePath), true))
      writer.println(logMessage)
      writer.close()
    }

    def info(message: String): Unit = log("INFO", message)
    def error(message: String): Unit = log("ERROR", message)
  }


  SimpleLogger.info("Starting the rules engine")

  val discountRules = List(
    ((order: Order) => true, (order: Order) => DiscountRules.expiryBasedDiscount(order)),
    ((order: Order) => true, (order: Order) => DiscountRules.cheeseWineDiscount(order)),
    ((order: Order) => true, (order: Order) => DiscountRules.specialDiscount(order)),
    ((order: Order) => true, (order: Order) => DiscountRules.quantityBasedDiscount(order)),
    ((order: Order) => true, (order: Order) => DiscountRules.appBasedDiscount(order)),
    ((order: Order) => true, (order: Order) => DiscountRules.visaCardDiscount(order))
  )

  val orders = OrderReader.readOrdersFromCSV("src\\main\\resources\\TRX1000.csv")
  SimpleLogger.info(s"Read ${orders.length} orders from the CSV")

  val processedOrders = orders.map(order => DiscountProcessor.getOrderWithDiscount(order, discountRules))
  SimpleLogger.info(s"Processed ${processedOrders.length} orders")

  processedOrders.foreach { order =>
    println(
      s"Order timestamp: ${order.timestamp}, Product: ${order.productName}, Original Price: ${order.originalPrice}, Discount: ${order.discount}%, Final Price: ${order.finalPrice}"
    )
  }

  Try(Class.forName("oracle.jdbc.OracleDriver")) match {
    case Success(_) =>
      SimpleLogger.info("Oracle JDBC driver loaded successfully")
    case Failure(e) =>
      SimpleLogger.error(s"Failed to load Oracle JDBC driver: ${e.getMessage}")
  }


  // Database connection setup
  val url = "jdbc:oracle:thin:@localhost:1521:XE"
  val username = "scala"
  val password = "123"
  val connection = Try(DriverManager.getConnection(url, username, password))


  connection match {
    case Success(con) =>
      SimpleLogger.info("Connected to the Oracle database")
      Orderwriter.writeOrdersToDatabase(con, processedOrders)
      con.close() // Close connection
      SimpleLogger.info("Database connection closed")
    case Failure(e) =>
      SimpleLogger.error(s"Could not connect to the database: ${e.getMessage}")
  }
}