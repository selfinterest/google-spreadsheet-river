/**
 * Created by terrence on 12/28/14.
 */
describe("Main module", function(){
    beforeEach(function(){

        var gsFactory = {};

        gsFactory.email = sinon.stub().returns(gsFactory);
        gsFactory.keyFile = sinon.stub().returns(gsFactory);
        gsFactory.https = sinon.stub().returns(gsFactory);
        gsFactory.spreadsheetName = sinon.stub().returns(gsFactory);
        gsFactory.worksheetName = sinon.stub().returns(gsFactory);

        this.gsStub = {factory: gsFactory, '@noCallThru': false};


        this.main = proxyquire("../lib/main.js", {'google-spreadsheet-stream-reader': this.gsStub});
    });
    it("should be instantiable", function(){
        var river = this.main.createRiver();
        expect(river).to.not.be.undefined;
        expect(this.gsStub.factory.email).to.have.been.called;
    });

    it("should be configurable", function(){
        this.main
            .expires(100)
            .email("bob@bob.com")
            .keyFile("../primary-documents-key-file.pem")
        ;

        var expires = this.main.expires();
        expect(expires).to.equal(100);
        expect(this.main.email()).to.equal("bob@bob.com");

    });
});