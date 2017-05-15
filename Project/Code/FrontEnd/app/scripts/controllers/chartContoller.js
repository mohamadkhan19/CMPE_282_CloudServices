'use strict';
/**
 * @ngdoc function
 * @name sbAdminApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the sbAdminApp
 */


angular.module('sbAdminApp')
  .controller('ChartCtrl', ['$scope', '$http' , '$timeout', function ($scope, $http, $timeout) {
    /*$scope.line = {
	    labels: ['January', 'February', 'March', 'April', 'May', 'June', 'July'],
	    series: ['Series A', 'Series B'],
	    data: [
	      [65, 59, 80, 81, 56, 55, 40],
	      [28, 48, 40, 19, 86, 27, 90]
	    ],
	    onClick: function (points, evt) {
	      console.log(points, evt);
	    }
    };*/

    $scope.line_function1 = function() {
        //Get sensors type
        //var data = APP_CLOUD.getHeaders(true);
        $http({
          method: 'GET',
          url: 'https://thinktank-backend.herokuapp.com/product_prices'
        }).then(function(resp) {
         // console.log("This is response: "+resp.data.obj[1].sumofproducts);
                $scope.totalPrice = resp.data;
                var temp1 = [ ];
                var temp2 = [ ];
                $scope.lables = [];
                $scope.data = [];
                angular.forEach($scope.totalPrice.obj,function(item){
                   temp1.push(item.product_id);
                   temp2.push(item.sum_product_price_ok);
               });

            
            return  $scope.line1 = {
                labels : temp1,
                data : [temp2],
                series: ['']
            };
        });


    };

    $scope.bar_function1 = function() {
        //Get sensors type
        //var data = APP_CLOUD.getHeaders(true);
        $http({
          method: 'GET',
          url: 'https://thinktank-backend.herokuapp.com/sales_year'
        }).then(function(resp) {
         // console.log("This is response: "+resp.data.obj[1].sumofproducts);
                $scope.totalPrice = resp.data;
                var temp1 = [ ];
                var temp2 = [ ];
                $scope.lables = [];
                $scope.data = [];
                angular.forEach($scope.totalPrice.obj,function(item){
                   temp1.push(item.year);
                   temp2.push(item.sumofproducts);
               });

            
            return  $scope.bar1 = {
                labels : temp1,
                data : [temp2],
                series: ['']
            };
        });


    };

    $scope.line_function2 = function() {
        //Get sensors type
        //var data = APP_CLOUD.getHeaders(true);
        $http({
          method: 'GET',
          url: 'https://thinktank-backend.herokuapp.com/supplier_quantity'
        }).then(function(resp) {
          console.log("This is response: "+resp.data.obj[1]);
                $scope.totalPrice = resp.data;
                var temp1 = [ ];
                var temp2 = [ ];
                $scope.lables = [];
                $scope.data = [];
                angular.forEach($scope.totalPrice.obj,function(item){
                   temp1.push(item.supplier_id);
                   temp2.push(item.cnt_quantity_ok);
               });

            
            return  $scope.line2 = {
                labels : temp1,
                data : [temp2],
                series: ['']
            };
        });


    };
    $scope.bar_function2 = function() {
        //Get sensors type
        //var data = APP_CLOUD.getHeaders(true);
        $http({
          method: 'GET',
          url: 'https://thinktank-backend.herokuapp.com/order_prices'
        }).then(function(resp) {
          //console.log("This is response: "+resp.data.obj[1].sumofproducts);
                $scope.totalPrice = resp.data;
                var temp1 = [ ];
                var temp2 = [ ];
                $scope.lables = [];
                $scope.data = [];
                angular.forEach($scope.totalPrice.obj,function(item){
                   temp1.push(item.customer_id);
                   temp2.push(item.avg_order_price_ok);
               });

            
            return  $scope.bar2 = {
                labels : temp1,
                data : [temp2],
                series: ['']
            };
        });


    };


    /*$scope.bar = {
        labels: ['2006', '2007', '2008', '2009', '2010', '2011', '2012'],
        series: ['Series A', 'Series B'],

        data: [
           [65, 59, 80, 81, 56, 55, 40],
           [28, 48, 40, 19, 86, 27, 90]
        ]
        
    };*/

    /*$scope.donut = {
    	labels: ["Download Sales", "In-Store Sales", "Mail-Order Sales"],
    	data: [300, 500, 100]
    };

    $scope.radar = {
    	labels:["Eating", "Drinking", "Sleeping", "Designing", "Coding", "Cycling", "Running"],

    	data:[
    	    [65, 59, 90, 81, 56, 55, 40],
    	    [28, 48, 40, 19, 96, 27, 100]
    	]
    };

    $scope.pie = {
    	labels : ["Download Sales", "In-Store Sales", "Mail-Order Sales"],
    	data : [300, 500, 100]
    };

    $scope.polar = {
    	labels : ["Download Sales", "In-Store Sales", "Mail-Order Sales", "Tele Sales", "Corporate Sales"],
    	data : [300, 500, 100, 40, 120]
    };

    $scope.dynamic = {
    	labels : ["Download Sales", "In-Store Sales", "Mail-Order Sales", "Tele Sales", "Corporate Sales"],
    	data : [300, 500, 100, 40, 120],
    	type : 'PolarArea',

    	toggle : function () 
    	{
    		this.type = this.type === 'PolarArea' ?
    	    'Pie' : 'PolarArea';
		}
    };
*/
    
}]);