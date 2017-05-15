'use strict';
/**
 * @ngdoc function
 * @name sbAdminApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the sbAdminApp
 */
angular.module('sbAdminApp')
  .controller('FormCtrl',['$scope', '$http' , '$timeout', function ($scope, $http, $timeout) {

  	$scope.login_function = function() {
      console.log("Success ");
        //Get sensors type
        //var data = APP_CLOUD.getHeaders(true);
        $http({
          method: 'POST',
          url: 'https://thinktank-backend.herokuapp.com/auth/login',
          data:{
                username: $("#username")[0].value, 
                password: $("#password")[0].value
            }
        }).then(function(resp) {
          if(resp.data.Success==1){

        	window.location = '/#/dashboard/chart';
        }
        else
          console.log("Fail login");
        });


    };
  }]);