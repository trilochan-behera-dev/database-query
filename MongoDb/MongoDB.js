//  categories                  ||     categoryID,categoryName,description,picture

//  customers                   ||    customerID,companyName,contactName,contactTitle,address,city,region,postalCode,country,phone,fax

//  employee_territories        ||    employeeID,territoryID

//  employees                   ||    employeeID,lastName,firstName,title,titleOfCourtesy,birthDate,hireDate,address,city,region,postalCode,country,homePhone,extension,photo,notes,reportsTo,photoPath

//  order_details               ||    orderID,productID,unitPrice,quantity,discount

//  orders                      ||    orderID,customerID,employeeID,orderDate,requiredDate,shippedDate,shipVia,freight,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry

//  products                    ||    productID,productName,supplierID,categoryID,quantityPerUnit,unitPrice,unitsInStock,unitsOnOrder,reorderLevel,discontinued

//  regions                     ||    regionID,regionDescription

//  shippers                    ||    shipperID,companyName,phone

//  suppliers                   ||    supplierID,companyName,contactName,contactTitle,address,city,region,postalCode,country,phone,fax,homePage

//  territories                 ||    territoryID,territoryDescription,regionID


//  Questions and Queries



//  1) Which shipper do we use the most to ship our orders out through?

db.orders.aggregate([
  { $group: { _id: "$shipVia", total_orders: { $sum: 1 } } },
  { $lookup: { from: "shippers", localField: "_id", foreignField: "shipperID", as: "shipper" } },
  { $unwind: "$shipper" },
  { $sort: { total_orders: -1 } },
  { $limit: 1 },
  { $project: { shipperID: "$shipper.shipperID", companyName: "$shipper.companyName", total_orders: 1, _id: 0 } }
]);



// 2) List the following employee information (EmployeeID, LastName, FirstName, ManagerLastName, ManagerFirstName)

db.employees.aggregate([
  {
    $lookup: {
      from: "employees",
      localField: "reportsTo",
      foreignField: "employeeID",
      as: "manager"
    }
  },
  {
    $project: {
      employeeID: 1,
      lastName: 1,
      firstName: 1,
      ManagerLastName: { $arrayElemAt: ["$manager.lastName", 0] },
      ManagerFirstName: { $arrayElemAt: ["$manager.firstName", 0] },
      _id: 0
    }
  }
]);


// 3) What are the last names of all employees who were born in November?

db.employees.find({ birthDate: { $regex: "-11-" } }, { lastName: 1, _id: 0 });


// 4) List each employee (lastname, firstname, territory) and sort the list by territory and then by employee's last name. Remember, employees may work for more than one territory.

db.employees.aggregate([
  {
    $lookup: {
      from: "employee_territories",
      localField: "employeeID",
      foreignField: "employeeID",
      as: "et"
    }
  },
  { $unwind: "$et" },
  {
    $lookup: {
      from: "territories",
      localField: "et.territoryID",
      foreignField: "territoryID",
      as: "territory"
    }
  },
  { $unwind: "$territory" },
  {
    $project: {
      lastName: 1,
      firstName: 1,
      territory: "$territory.territoryDescription",
      _id: 0
    }
  },
  { $sort: { territory: 1, lastName: 1 } }
]);


// 5) Regarding sales value, what has been our best-selling product of all time?

db.order_details.aggregate([
  { $group: { _id: "$productID", total_quantity: { $sum: "$quantity" } } },
  { $lookup: { from: "products", localField: "_id", foreignField: "productID", as: "product" } },
  { $unwind: "$product" },
  { $sort: { total_quantity: -1 } },
  { $limit: 1 },
  { $project: { productID: "$product.productID", productName: "$product.productName", total_quantity: 1, _id: 0 } }
]);


// 6) Regarding sales value, this only includes products that have at least been sold once, which has been our worst-selling product of all time?

db.order_details.aggregate([
  { $group: { _id: "$productID", total_quantity: { $sum: "$quantity" } } },
  { $lookup: { from: "products", localField: "_id", foreignField: "productID", as: "product" } },
  { $unwind: "$product" },
  { $match: { total_quantity: { $gt: 0 } } },
  { $sort: { total_quantity: 1 } },
  { $limit: 1 },
  { $project: { productID: "$product.productID", productName: "$product.productName", total_quantity: 1, _id: 0 } }
]);


// 7) Regarding sales value, which month has been traditionally best for sales?

db.order_details.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "orderID",
      foreignField: "orderID",
      as: "order"
    }
  },
  { $unwind: "$order" },
  {
    $group: {
      _id: { $month: "$order.orderDate" },
      total_sales: { $sum: { $multiply: ["$unitPrice", "$quantity"] } }
    }
  },
  { $sort: { total_sales: -1 } },
  { $limit: 1 },
  { $project: { month: "$_id", total_sales: 1, _id: 0 } }
]);


// 8) What is the name of our best salesperson?

db.order_details.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "orderID",
      foreignField: "orderID",
      as: "order"
    }
  },
  { $unwind: "$order" },
  {
    $lookup: {
      from: "employees",
      localField: "order.employeeID",
      foreignField: "employeeID",
      as: "employee"
    }
  },
  { $unwind: "$employee" },
  {
    $group: {
      _id: {
        employeeID: "$employee.employeeID",
        lastName: "$employee.lastName",
        firstName: "$employee.firstName"
      },
      total_sales: { $sum: { $multiply: ["$quantity", "$unitPrice"] } }
    }
  },
  { $sort: { total_sales: -1 } },
  { $limit: 1 },
  {
    $project: {
      employeeID: "$_id.employeeID",
      lastName: "$_id.lastName",
      firstName: "$_id.firstName",
      total_sales: 1,
      _id: 0
    }
  }
]);



// 9) Product report (productID, ProductName, SupplierName, ProductCategory). Order the list by product category.

db.products.aggregate([
  {
    $lookup: {
      from: "suppliers",
      localField: "supplierID",
      foreignField: "supplierID",
      as: "supplier"
    }
  },
  { $unwind: "$supplier" },
  {
    $lookup: {
      from: "categories",
      localField: "categoryID",
      foreignField: "categoryID",
      as: "category"
    }
  },
  { $unwind: "$category" },
  {
    $project: {
      productID: 1,
      productName: 1,
      supplierName: "$supplier.companyName",
      productCategory: "$category.categoryName",
      _id: 0
    }
  },
  { $sort: { productCategory: 1 } }
]);


// 10) Produce a count of the employees by each sales region.

db.employees.aggregate([
  {
    $lookup: {
      from: "territories",
      localField: "employeeID",
      foreignField: "employeeID",
      as: "territories"
    }
  },
  { $unwind: "$territories" },
  {
    $lookup: {
      from: "regions",
      localField: "territories.regionID",
      foreignField: "regionID",
      as: "region"
    }
  },
  { $unwind: "$region" },
  {
    $group: {
      _id: "$region.regionDescription",
      employeeCount: { $sum: 1 }
    }
  }
]);


// 11) List the dollar values for sales by region.

db.orders.aggregate([
  {
    $lookup: {
      from: "order_details",
      localField: "orderID",
      foreignField: "orderID",
      as: "orderDetails"
    }
  },
  {
    $unwind: "$orderDetails"
  },
  {
    $lookup: {
      from: "territories",
      localField: "employeeID",
      foreignField: "employeeID",
      as: "territory"
    }
  },
  {
    $unwind: "$territory"
  },
  {
    $lookup: {
      from: "regions",
      localField: "territory.regionID",
      foreignField: "regionID",
      as: "region"
    }
  },
  {
    $unwind: "$region"
  },
  {
    $group: {
      _id: "$region.regionDescription",
      totalSales: { $sum: { $multiply: ["$orderDetails.unitPrice", "$orderDetails.quantity"] } }
    }
  },
  {
    $project: {
      regionDescription: "$_id",
      totalSales: 1,
      _id: 0
    }
  }
]);


// 12) · What is the average value of a sales order?

db.order_details.aggregate([
  {
    $group: {
      _id: null,
      average_order_value: { $avg: { $multiply: ["$unitPrice", "$quantity"] } }
    }
  },
  { $project: { _id: 0 } }
]);



// 13) List orders (OrderID, OrderDate, CustomerName) where the total order value exceeds a sales order's average value.

var averageOrderValue = db.order_details.aggregate([
  {
    $group: {
      _id: null,
      average_order_value: { $avg: { $multiply: ["$unitPrice", "$quantity"] } }
    }
  }
]).next().average_order_value;

db.orders.aggregate([
  {
    $lookup: {
      from: "customers",
      localField: "customerID",
      foreignField: "customerID",
      as: "customer"
    }
  },
  { $unwind: "$customer" },
  {
    $lookup: {
      from: "order_details",
      localField: "orderID",
      foreignField: "orderID",
      as: "order_details"
    }
  },
  {
    $project: {
      orderID: 1,
      orderDate: 1,
      customerName: "$customer.companyName",
      total_order_value: { $sum: { $multiply: ["$order_details.unitPrice", "$order_details.quantity"] } },
      _id: 0
    }
  },
  { $match: { total_order_value: { $gt: averageOrderValue } } }
]);



// 14) Produce a customer report (must also include those we have not yet done business with) showing CustomerID, Customer name, and total sales made to that customer.

db.customers.aggregate([
  {
    $lookup: {
      from: "orders",
      localField: "customerID",
      foreignField: "customerID",
      as: "orders"
    }
  },
  { $unwind: { path: "$orders", preserveNullAndEmptyArrays: true } },
  {
    $lookup: {
      from: "order_details",
      localField: "orders.orderID",
      foreignField: "orderID",
      as: "order_details"
    }
  },
  {
    $group: {
      _id: "$customerID",
      companyName: { $first: "$companyName" },
      total_sales: { $sum: { $multiply: ["$order_details.unitPrice", "$order_details.quantity"] } }
    }
  }
]);


// 15) List all products that need to be re-ordered. Do not include discontinued products in this report.

db.products.find({ unitsInStock: { $lte: "$reorderLevel" }, discontinued: { $ne: true } }, { productID: 1, productName: 1, unitsInStock: 1, _id: 0 });
