-- tables and columns

-- categories                  ||     categoryID,categoryName,description,picture

-- customers                   ||    customerID,companyName,contactName,contactTitle,address,city,region,postalCode,country,phone,fax

-- employee_territories        ||    employeeID,territoryID

-- employees                   ||    employeeID,lastName,firstName,title,titleOfCourtesy,birthDate,hireDate,address,city,region,postalCode,country,homePhone,extension,photo,notes,reportsTo,photoPath

-- order_details               ||    orderID,productID,unitPrice,quantity,discount

-- orders                      ||    orderID,customerID,employeeID,orderDate,requiredDate,shippedDate,shipVia,freight,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry

-- products                    ||    productID,productName,supplierID,categoryID,quantityPerUnit,unitPrice,unitsInStock,unitsOnOrder,reorderLevel,discontinued

-- regions                     ||    regionID,regionDescription

-- shippers                    ||    shipperID,companyName,phone

-- suppliers                   ||    supplierID,companyName,contactName,contactTitle,address,city,region,postalCode,country,phone,fax,homePage

-- territories                 ||    territoryID,territoryDescription,regionID


-- Questions and Queries



-- 1) Which shipper do we use the most to ship our orders out through?

SELECT shipperID, companyName, COUNT(*) AS total_orders
FROM orders
JOIN shippers ON orders.shipVia = shippers.shipperID
GROUP BY shipperID
ORDER BY total_orders DESC
LIMIT 1;


-- 2) List the following employee information (EmployeeID, LastName, FirstName, ManagerLastName, ManagerFirstName)

SELECT e.employeeID, e.lastName, e.firstName, m.lastName AS managerLastName, m.firstName AS managerFirstName
FROM employees e
LEFT JOIN employees m ON e.reportsTo = m.employeeID;


-- 3) What are the last names of all employees who were born in November?

SELECT lastName
FROM employees
WHERE MONTH(birthDate) = 11;


-- 4) List each employee (lastname, firstname, territory) and sort the list by territory and then by employee's last name. Remember, employees may work for more than one territory.

SELECT e.lastName, e.firstName, t.territoryDescription AS territory
FROM employees e
JOIN employee_territories et ON e.employeeID = et.employeeID
JOIN territories t ON et.territoryID = t.territoryID
ORDER BY territory, e.lastName;


-- 5) Regarding sales value, what has been our best-selling product of all time?

SELECT p.productID, p.productName, SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS total_sales
FROM products p
JOIN order_details od ON p.productID = od.productID
GROUP BY p.productID, p.productName
ORDER BY total_sales DESC
LIMIT 1;


-- 6) Regarding sales value, this only includes products that have at least been sold once, which has been our worst-selling product of all time?

SELECT p.productID, p.productName, SUM(od.quantity * od.unitPrice * (1 - od.discount)) AS total_sales
FROM products p
LEFT JOIN order_details od ON p.productID = od.productID
GROUP BY p.productID, p.productName
ORDER BY total_sales ASC
LIMIT 1;


-- 7) Regarding sales value, which month has been traditionally best for sales?

SELECT MONTHNAME(orderDate) AS best_month, SUM(order_details.quantity * order_details.unitPrice * (1 - order_details.discount)) AS total_sales
FROM orders
JOIN order_details ON orders.orderID = order_details.orderID
GROUP BY best_month
ORDER BY total_sales DESC
LIMIT 1;


-- 8) What is the name of our best salesperson?

SELECT e.employeeID, e.lastName, e.firstName, SUM(order_details.quantity * order_details.unitPrice * (1 - order_details.discount)) AS total_sales
FROM employees e
JOIN orders ON e.employeeID = orders.employeeID
JOIN order_details ON orders.orderID = order_details.orderID
GROUP BY e.employeeID, e.lastName, e.firstName
ORDER BY total_sales DESC
LIMIT 1;


-- 9) Product report (productID, ProductName, SupplierName, ProductCategory). Order the list by product category.

SELECT p.productID, p.productName, s.companyName AS supplierName, c.categoryName AS productCategory
FROM products p
JOIN suppliers s ON p.supplierID = s.supplierID
JOIN categories c ON p.categoryID = c.categoryID
ORDER BY productCategory;


-- 10) Produce a count of the employees by each sales region.

SELECT r.regionDescription AS salesRegion, COUNT(*) AS employeeCount
FROM employees e
JOIN territories t ON e.employeeID = t.employeeID
JOIN regions r ON t.regionID = r.regionID
GROUP BY salesRegion;


-- 11) List the dollar values for sales by region.

SELECT r.regionDescription AS salesRegion, SUM(order_details.quantity * order_details.unitPrice * (1 - order_details.discount)) AS total_sales
FROM orders o
JOIN order_details ON o.orderID = order_details.orderID
JOIN territories t ON o.employeeID = t.employeeID
JOIN regions r ON t.regionID = r.regionID
GROUP BY salesRegion;


-- 12) · What is the average value of a sales order?

SELECT AVG(unitPrice * quantity) AS average_order_value
FROM order_details;


-- 13) List orders (OrderID, OrderDate, CustomerName) where the total order value exceeds a sales order's average value.

SELECT o.orderID, o.orderDate, c.companyName AS customerName
FROM orders AS o
JOIN customers AS c ON o.customerID = c.customerID
JOIN (
  SELECT AVG(unitPrice * quantity) AS average_order_value
  FROM order_details
) AS avg ON (SELECT SUM(unitPrice * quantity) FROM order_details WHERE orderID = o.orderID) > avg.average_order_value;


-- 14) Produce a customer report (must also include those we have not yet done business with) showing CustomerID, Customer name, and total sales made to that customer.

SELECT c.customerID, c.companyName, COALESCE(SUM(od.unitPrice * od.quantity), 0) AS total_sales
FROM customers AS c
LEFT JOIN orders AS o ON c.customerID = o.customerID
LEFT JOIN order_details AS od ON o.orderID = od.orderID
GROUP BY c.customerID, c.companyName;

-- 15) List all products that need to be re-ordered. Do not include discontinued products in this report.

SELECT p.productID, p.productName, p.unitsInStock
FROM products AS p
WHERE p.unitsInStock <= p.reorderLevel AND p.discontinued = 0;
