(function() {
    var Flower = {
        jobs: [],
        current_job: false,
        current_job_id: false,
        key: false,
        api_base: '/'
    }
    var app = angular.module('crowdLauncher', [
        'flower-directives',
        'ngRoute'
    ]);
    // ====================================================================
    // Data collection functions
    // ====================================================================
    var getJobs = function($http, callback) {
        $http.post(Flower.api_base + 'jobs', {
            api_key: Flower.key
        }).success(function(data) {
            console.log(data);
            if (data.length > 0) {
                Flower.jobs = data;
                callback();
            } else {
                alert('No jobs are found');
            }
        });
    }
    var getJob = function($http, jobId, callback) {
        $http.post(Flower.api_base + 'jobs/'+jobId, {
            api_key: Flower.key
        }).success(function(data) {
            console.log(data);
            Flower.current_job = data;
            callback();
        });
    }
    // ====================================================================
    // Controllers
    // ====================================================================
    app.controller('ApiController', ['$scope', '$http',
        function($scope, $http) {
            // Function to be called when pressing ENTER
            $scope.submitForm = function() {
                Flower.key = $scope.api_key;
                console.log($scope.api_key);

                getJobs($http,function(){
                    window.location = "#/jobs";
                });
            };
        }
    ]);
    app.controller('JobsController', ['$scope', '$http',

        function($scope, $http) {
            console.log($scope);
            if (Flower.jobs.length == 0) {
                window.location = "#/"
            }
            $scope.jobs = Flower.jobs;
        }
    ]);

    app.controller('JobController', ['$scope', '$http', '$routeParams',
        function($scope, $http, $routeParams){
            if (!Flower.key) {
                window.location = "#/"
            }
            Flower.current_job_id = $routeParams.jobId;
            getJob($http,Flower.current_job_id,function(){
                $scope.current_job = Flower.current_job;
            });
        }
    ]);
    // ====================================================================
    // Routers
    // ====================================================================
    app.config(['$routeProvider',
        function($routeProvider) {
            $routeProvider.
            when('/', {
                templateUrl: 'views/hero.html',
                controller: 'ApiController'
            }).
            when('/jobs', {
                templateUrl: 'views/job-list.html',
                controller: 'JobsController'
            }).
            when('/jobs/:jobId', {
                templateUrl: 'views/job-detail.html',
                controller: 'JobController'
            }).
            otherwise({
                redirectTo: '/'
            });
        }
    ]);

})();
