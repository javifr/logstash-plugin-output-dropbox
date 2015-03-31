require 'open-uri'
require 'net/https'

module Net
  class HTTP
    alias_method :original_ciphers=, :ciphers=

    def ciphers=(ciphs)
      if Dropbox::TRUSTED_CERT_FILE.eql?(self.ca_file)
        # special hack to fix a problem with dropbox gem 1.6.4 under JRuby 1.7.12
        # dropbox sdk set ciphers not suitable for JRuby SSLContext, which would
        # cause exception ...
        original_ciphers = nil
      else
        original_ciphers = ciphs
      end
    end
  end
end