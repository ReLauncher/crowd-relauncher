# ReLauncher – Server side only
[![Build Status](https://travis-ci.org/pavelk2/crowd-relauncher.svg)](https://travis-ci.org/pavelk2/crowd-relauncher)
[![Code Climate](https://codeclimate.com/github/pavelk2/crowd-relauncher/badges/gpa.svg)](https://codeclimate.com/github/pavelk2/crowd-relauncher)

A runtime controller, which cancels and relaunches delayed units on [CrowdFlower](http://www.crowdflower.com) to improve the overall task execution time.

An example of an execution timeline for a task with 20 units on CrowdFlower:
![](Web/img/abstract.png)

A comparison of a task with 100 units ran 3 times *without* and 3 times *with* **ReLauncher**:
![](Web/img/comparison.png)

You can deploy ReLauncher on Heroku in one-click for free:

[![Deploy](https://www.herokucdn.com/deploy/button.png)](https://heroku.com/deploy)

#### [Checkout out the online demo](https://crowd-relauncher.herokuapp.com)
To use the demo you need to enter your CrowdFlower API Key, you can find [here](https://make.crowdflower.com/account/user). This API key is not stored anywhere and is used only to send requests to CrowdFlower from your account.

#### Publication
The paper describing the approach is accepted to [CSCW2016 conference in San Francisco](http://cscw.acm.org/2016/index.php)


