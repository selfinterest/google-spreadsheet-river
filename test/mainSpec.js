/**
 * Created by terrence on 12/28/14.
 */
describe("Main module", function(){
    beforeEach(function(){
        this.main = require("../lib/main.js");
    });
    it("should be instantiable", function(){
        var river = this.main.createRiver();
        expect(river).to.not.be.undefined;
    });

    it("should be configurable", function(){
        this.main
            .expires(100)
            .email("bob@bob.com")
        ;

        var expires = this.main.expires();
        expect(expires).to.equal(100);
        expect(this.main.email()).to.equal("bob@bob.com");
    });
});