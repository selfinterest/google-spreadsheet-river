/**
 * Created by terrence on 12/28/14.
 */

global.chai = require("chai");
global.sinon = require("sinon");
global.expect = chai.expect;
global.proxyquire = require("proxyquire");

var sinonChai = require("sinon-chai");

chai.use(sinonChai);