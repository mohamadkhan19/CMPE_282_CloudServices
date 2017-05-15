var Redshift = require('node-redshift');
var client = require('../client');
var redshift = new Redshift(client);



exports.sales_year = function(req, res){
  var statement = 'SELECT SUM("products"."product_price") AS "sumofProducts", ' +
'  CAST(EXTRACT(YEAR FROM CAST("customer_orders"."date_order_paid" AS TIMESTAMP WITHOUT TIME ZONE)) AS INTEGER) AS ' + 
' "year" ' +
'FROM "public"."products" "products" ' +
'  INNER JOIN "public"."customer_orders_product" "customer_orders_product" ON ("products"."product_id" = ' +
' "customer_orders_product"."order_id") ' +
'  INNER JOIN "public"."customer_orders" "customer_orders" ON ("customer_orders_product"."order_id" = "customer_orders"."order_id") ' +
'  INNER JOIN "public"."customer_orders_delivery" "customer_orders_delivery" ON ("customer_orders"."order_id" = ' +
' "customer_orders_delivery"."order_id") ' +
'  INNER JOIN "public"."customer_addresses" "customer_addresses" ON ("customer_orders"."customer_id" = "customer_addresses"."customer_id") ' +
'WHERE ((NOT ("products"."supplier_id" IS NULL)) AND ((NOT ("customer_orders"."customer_payement_method_id" IS NULL)) AND (NOT ' +
' ("customer_orders"."customer_id" IS NULL)))) ' +
' GROUP BY 2'+
' ORDER BY "year" ASC'

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error Selecting : %s ",err );
  else{
    console.log(data);
    res.status(200).json({
                message: 'Success',
                obj: data
                });
  }
});
};



exports.count = function(req, res){

  var statement = 'select count(*) from customer_orders';

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error getting countd : %s ",err );
  else{
    console.log(data);
    res.status(200).json({
                message: 'Success',
                Success: 1,
                object: data
                });
  }
});
};



exports.login = function(req, res){

  var username = req.body.username,
      password = req.body.password,
      respUsername=null;

  var statement = 'SELECT * from login where username= \''+username+'\' and password = \''+password+'\';'

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error Selecting : %s ",err );
  else{
    //console.log(data[0].username);

    if (data!="" && data != null && data[0].username==username){
      res.status(200).json({
                Success: 1
                });
    }
    else{
      res.status(200).json({
                Success: 0
                });
    }
}
});
};

exports.supplier_quantity = function(req, res){
  var statement = 'SELECT COUNT("customer_orders_product"."quantity") AS "cnt_quantity_ok",'+
  '"products"."supplier_id" AS "supplier_id"'+
'FROM "public"."products" "products"'+
  'INNER JOIN "public"."customer_orders_product" "customer_orders_product" ON ("products"."product_id" = "customer_orders_product"."order_id")'+
  'INNER JOIN "public"."customer_orders" "customer_orders" ON ("customer_orders_product"."order_id" = "customer_orders"."order_id")'+
  'INNER JOIN "public"."customer_orders_delivery" "customer_orders_delivery" ON ("customer_orders"."order_id" = "customer_orders_delivery"."order_id")'+
  'INNER JOIN "public"."customer_addresses" "customer_addresses" ON ("customer_orders"."customer_id" = "customer_addresses"."customer_id")'+
'WHERE ((NOT ("products"."supplier_id" IS NULL)) AND ((NOT ("customer_orders"."customer_payement_method_id" IS NULL)) AND (NOT ("customer_orders"."customer_id" IS NULL))))'+
'GROUP BY 2'+
'ORDER BY "supplier_id" ASC;'

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error Selecting : %s ",err );
  else{
    console.log(data);
    res.status(200).json({
                message: 'Success',
                obj: data
                });
  }
});
};

exports.product_prices = function(req, res){
  var statement = 'SELECT "products"."product_id" AS "product_id",'+
  'SUM("products"."product_price") AS "sum_product_price_ok"'+
'FROM "public"."products" "products"'+
  'INNER JOIN "public"."customer_orders_product" "customer_orders_product" ON ("products"."product_id" = "customer_orders_product"."order_id")'+
  'INNER JOIN "public"."customer_orders" "customer_orders" ON ("customer_orders_product"."order_id" = "customer_orders"."order_id")'+
  'INNER JOIN "public"."customer_orders_delivery" "customer_orders_delivery" ON ("customer_orders"."order_id" = "customer_orders_delivery"."order_id")'+
  'INNER JOIN "public"."customer_addresses" "customer_addresses" ON ("customer_orders"."customer_id" = "customer_addresses"."customer_id")'+
'WHERE ((NOT ("products"."supplier_id" IS NULL)) AND ((NOT ("customer_orders"."customer_payement_method_id" IS NULL)) AND (NOT ("customer_orders"."customer_id" IS NULL))))'+
'GROUP BY 1'+
'ORDER BY "product_id" ASC;'

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error Selecting : %s ",err );
  else{
    console.log(data);
    res.status(200).json({
                message: 'Success',
                obj: data
                });
  }
});
};

exports.order_prices = function(req, res){
  var statement = 'SELECT AVG(CAST("customer_orders"."order_price" AS DOUBLE PRECISION)) AS "avg_order_price_ok",'+
  '"customer_orders"."customer_id" AS "customer_id"'+
'FROM "public"."products" "products"'+
 'INNER JOIN "public"."customer_orders_product" "customer_orders_product" ON ("products"."product_id" = "customer_orders_product"."order_id")'+
  'INNER JOIN "public"."customer_orders" "customer_orders" ON ("customer_orders_product"."order_id" = "customer_orders"."order_id")'+
  'INNER JOIN "public"."customer_orders_delivery" "customer_orders_delivery" ON ("customer_orders"."order_id" = "customer_orders_delivery"."order_id")'+
  'INNER JOIN "public"."customer_addresses" "customer_addresses" ON ("customer_orders"."customer_id" = "customer_addresses"."customer_id")'+
'WHERE ((NOT ("products"."supplier_id" IS NULL)) AND ((NOT ("customer_orders"."customer_payement_method_id" IS NULL)) AND (NOT ("customer_orders"."customer_id" IS NULL))))'+
'GROUP BY 2'+
'ORDER BY "customer_id" ASC;'

redshift.query(statement, {raw: true}, function(err, data){
  if(err) 
    console.log("Error Selecting : %s ",err );
  else{
    console.log(data);
    res.status(200).json({
                message: 'Success',
                obj: data
                });
  }
});
};
