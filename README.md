# Scala-Rule-Engine

**Project Overview**
The system reads order data from a CSV file, applies various discount rules, and calculates the final price after applying discounts. It then writes the processed data to an Oracle database. The system is designed with a functional programming approach, ensuring immutability and clear code structure.

Key features include:

*-Multiple Discount Rules: The system supports various discount rules that are applied to the orders to calculate the final price. These rules cover a range of conditions like expiry date, product type, special dates, quantity purchased, sales channel, and payment method.

*-Functional Programming: The project is developed using a functional programming paradigm, avoiding mutable variables and explicit loops. This approach improves code maintainability and reduces side effects.

*-Oracle Database Integration: The system writes processed orders to an Oracle database, allowing for further analysis and data persistence. This integration requires appropriate database connection setup.

*-Logging: The system logs significant events (like database connection success, order processing, and errors) to a text file (rules_engine.log). The log entries are formatted with a timestamp, log level, and a descriptive message.


**Discount Rules**
The following discount rules are applied to the orders to calculate discounts:

--Expiry-based Discount: A discount that varies based on the number of days until the product expires. This discount decreases as the product gets closer to its expiry date.
Formula: 
1%×(30−days to expiry)
1%×(30−days to expiry),
 for products with less than 30 days until expiry.

--Product-based Discount: Special discounts for specific types of products.
*Cheese: 10% discount for cheese-based products.
*Wine: 5% discount for wine-based products.

--Date-based Discount: A unique discount for transactions occurring on the 23rd of March, offering a significant discount to celebrate a special event.
50% discount for all transactions on this date.

--Quantity-based Discount: Discounts based on the quantity of units purchased.
*6-9 units: 5% discount.
*10-14 units: 7% discount.
*15 or more units: 10% discount.

--Channel-based Discount: A discount for sales made through a specific channel, such as a mobile app.
The discount is calculated based on rounding the quantity to the nearest multiple of 5.
Each multiple of 5 results in a 5% discount.

--Payment Method-based Discount: A discount for sales made using Visa cards to encourage card payments over cash.
5% discount for transactions paid with Visa cards.

**Project Structure**
The project is structured to maintain a clean, functional programming approach, with a focus on immutability and modularity. Here's a breakdown of the project's main components:

--Order: A case class representing the data for an order. This class contains fields like timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod, discount, and methods to calculate the original price and final price after applying discounts.

--DiscountRules: An object containing various discount rules used to calculate discounts for orders. These rules cover expiry-based discounts, product-based discounts, date-based discounts, quantity-based discounts, channel-based discounts, and payment method-based discounts.

--DiscountProcessor: An object responsible for applying the discount rules to each order and returning the updated order with a calculated discount. It processes a list of rules and applies them to the order to determine the appropriate discount.

--OrderReader: An object that reads and parses orders from a CSV file. It handles the extraction of order information from a CSV file, ensuring correct parsing and error handling.

--Orderwriter: An object that writes processed orders to an Oracle database. It prepares an SQL INSERT statement and inserts each order into the database, handling exceptions and logging errors if they occur.

--SimpleLogger: An object for logging significant events to a text file. It provides methods to log messages with different log levels (INFO, WARN, ERROR), ensuring proper resource management by closing the file after writing.

--Main: The main entry point to the project. This object coordinates reading orders from the CSV file, applying the discount rules, writing to the Oracle database, and logging significant events.

