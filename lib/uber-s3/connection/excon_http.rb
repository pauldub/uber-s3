require 'excon'
require 'timeout'

module UberS3::Connection
  class ExconHttp < Adapter
    def request(verb, url, headers={}, body=nil)
      if verb == :get
        # Support fetching compressed data
        headers['Accept-Encoding'] = 'gzip, deflate'
      end
      
      self.uri = URI.parse(url)

      # Init and open a HTTP connection
      http_connect! if http.nil?

      # Make HTTP request
      retries = 2
      begin
				res = http.request(:method 	 => verb,
													 :path 		 => uri.path,
													 :headers  => headers,
													 :body 		 => body)
      rescue EOFError, Errno::EPIPE => exp
        # Something happened to our connection, lets try this again
        http_connect!
        retries -= 1

        if retries >= 0
					retry
				else
					raise exp
				end
      end

      # Auto-decode any gzipped objects
      if verb == :get && res.get_header('Content-Encoding') == 'gzip'
        gz = Zlib::GzipReader.new(StringIO.new(res.body))
        response_body = gz.read
      else
        response_body = res.body
      end
      
      UberS3::Response.new({
        :status => res.status.to_i,
        :header => res.headers,
        :body   => response_body,
        :raw    => res
      })
    end

    private

      def http_connect!
        self.http = Excon.new(uri.to_s)
      end
    
  end
end
