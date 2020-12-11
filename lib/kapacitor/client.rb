require 'httpclient'
require 'json'

module Kapacitor
  class Client
    # @return [URI] Kapacitor REST API URL
    attr_reader :url
    # @return [Net::HTTP] HTTP client instance
    attr_reader :http

    # Create a new client
    #
    # @param url [String] Kapacitor REST API's URL (defaults to `http://localhost:9092`)
    # @param version [Integer] API version (defaults to `v1preview`)
    #
    def initialize(url: 'http://localhost:9092/kapacitor', version: 'v1')
      @http = HTTPClient.new
      @url = [url, version].join('/')
    end

    # Define a Kapacitor template
    #
    # @param id [String] Template ID
    # @param type [String] Template type. Valid values: `batch`, `stream`.
    # @param script [String] Tick script
    #
    def define_template(id:, type:, script:)
      raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless type == 'batch' || type == 'stream'

      req = {
        'id' => id,
        'type' => type,
        'script' => script
      }

      api_post(endpoint: 'templates', data: req)
    end

    # Update a Kapacitor template
    #
    # @param id [String] Template ID
    # @param **opts [Hash] Any number of template parameters to push into the Hash
    #
    def update_template(id:, **opts)
      req = {}
      req['type'] = opts[:type] if opts[:type]
      req['script'] = opts[:script] if opts[:script]

      if opts[:type]
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless opts[:type] == 'batch' or opts[:type] == 'stream'
      end

      api_patch(endpoint: "templates/#{id}", data: req) unless req.empty?
    end

    # Delete a Kapacitor template
    #
    # @param id [String] Template ID
    #
    def delete_template(id:)
      api_delete(endpoint: "templates/#{id}")
    end

    # Retrieve Kapacitor topic
    #
    # @return [List[String]] List of topics
    #
    def topics()
      res = api_get(endpoint: "alerts/topics")
      return res['topics'].map { |v| v['id'] }
    end

    # Retrieve Kapacitor templates
    #
    # @param offset [Integer] Offset count for paginating through templates
    # @param limit [Integer] Maximum number of templates to return
    # @return [Array[Hash]] List of templates
    #
    def templates(offset: 0, limit: 100)
      ret = []

      loop do
        res = api_get(endpoint: "templates?offset=#{offset}&limit=#{limit}")['templates']
        break unless res.size > 0
        ret += res
        offset += limit
      end

      ret
    end

    # Define a Kapacitor task
    #
    # @param id [String] Task ID
    # @param dbrps [String] List of database retention policy pairs the task is allowed to access.
    # @param **opts [Hash] Any number of task parameters to push into the Hash
    #
    def define_task(id:, dbrps:, **opts)
      if (opts[:template_id].nil? && opts[:type].nil? && opts[:script].nil?) || (opts[:template_id] && (opts[:type] || opts[:script]))
        raise ArgumentError, "Must specify either a Template ID or a script and type"
      elsif opts[:template_id].nil? && (opts[:type].nil? || opts[:script].nil?)
        raise ArgumentError, "Must specify both task type and script when not using a Template ID"
      end

      if opts[:status]
        raise ArgumentError, "Kapacitor task status can be either 'enabled' or 'disabled'" unless opts[:status] == 'enabled' || opts[:status] == 'disabled'
      end

      if opts[:type]
        raise ArgumentError, "Kapacitor task type can be either 'batch' or 'stream'" unless opts[:type] == 'batch' || opts[:type] == 'stream'
      end

      req = {
        'id' => id,
        'dbrps' => dbrps,
        'status' => opts[:status] || 'enabled'
      }

      if opts[:template_id]
        req['template-id'] = opts[:template_id]
      else
        req['type'] = opts[:type]
        req['script'] = opts[:script]
      end

      req['vars'] = opts[:vars] if opts[:vars]

      api_post(endpoint: 'tasks', data: req)
    end

    # Update a Kapacitor task
    #
    # @param id [String] Task ID
    # @param **opts [Hash] Any number of task parameters to push into the Hash
    #
    def update_task(id:, **opts)
      req = {}
      req['template-id'] = opts[:template_id] if opts[:template_id]
      req['type'] = opts[:type] if opts[:type]
      req['dbrps'] = opts[:dbrps] if opts[:dbrps]
      req['script'] = opts[:script] if opts[:script]
      req['status'] = 'disabled'
      req['vars'] = opts[:vars] if opts[:vars]

      if opts[:type]
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless opts[:type] == 'batch' || opts[:type] == 'stream'
      end

      if opts['status']
        raise ArgumentError, "Kapacitor task status can be either 'enabled' or 'disabled'" unless opts[:status] == 'enabled' || opts[:status] == 'disabled'
      end

      api_patch(endpoint: "tasks/#{id}", data: req) unless req.empty?

      if opts[:status] == 'enabled'
        req['status'] = opts[:status] if opts[:status]
        api_patch(endpoint: "tasks/#{id}", data: req) unless req.empty?
      end

    end

    # Delete a Kapacitor task
    #
    # @param id [String] Task ID
    #
    def delete_task(id:)
      api_delete(endpoint: "tasks/#{id}")
    end

    # Retrieve Kapacitor tasks
    #
    # @param offset [Integer] Offset count for paginating through tasks
    # @param limit [Integer] Maximum number of tasks to return
    # @return [Array[Hash]] List of tasks
    #
    def tasks(offset: 0, limit: 100)
      tasks = []

      loop do
        res = api_get(endpoint: "tasks?fields=id&offset=#{offset}&limit=#{limit}")['tasks']
        break unless res.size > 0

        res.each do |task|
          tasks << api_get(endpoint: "tasks/#{task['id']}")
        end

        offset += limit
      end

      tasks
    end

    # Define a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    # @param kind [String] Kind of handler
    # @param match [String] Lambda expression
    # @param options [Hash] Handler options
    #
    def define_topic_handler(id:, topic:, kind:, match: nil, options: {})
      req = {
        'id': id,
        'kind': kind
      }
      req['match'] = match unless match.nil?
      req['options'] = options
      api_post(endpoint: "alerts/topics/#{topic}/handlers", data: req)
    end

    # Update a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    # @param kind [String] Kind of handler
    # @param match [String] Lambda expression
    # @param options [Hash] Handler options
    #
    def update_topic_handler(id:, topic:, kind:, match: nil, options: nil)
      req = {
        'id': id,
        'kind': kind
      }
      req['match'] = match unless match.nil?
      req['options'] = options unless options.nil?
      api_put(endpoint: "alerts/topics/#{topic}/handlers/#{id}", data: req) unless req.empty?
    end

    # Delete a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    #
    def delete_topic_handler(id:, topic:)
      api_delete(endpoint: "alerts/topics/#{topic}/handlers/#{id}")
    end

    # Retrieve topic's handlers
    #
    # @param topic [String] Topic name
    # @return [Array[Hash]] List of handlers
    #
    def topic_handlers(topic:)
      api_get(endpoint: "alerts/topics/#{topic}/handlers")['handlers']
    end

private
    # Perform a HTTP GET request
    #
    # @param endpoint [String] HTTP API endpoint
    # @param query [String] HTTP query
    # @return [Array[Hash], Hash] API response
    #
    def api_get(endpoint:, query: nil)
      begin
        resp = self.http.get([self.url, endpoint].join('/'), query, {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        begin
          data = JSON.parse(resp.body) unless resp.body.empty?
        rescue JSON::ParserError
          raise Exception, "Failed to decode response message"
        end
        if resp.status != 200
          error = data.include?('error') ? data['error'] : data.inspect if data
          raise Exception, "Query returned a non successful HTTP code (Status: #{resp.status}, Reason: #{resp.reason}#{", Error: #{error}" if error}"
        end
      rescue
        raise Exception, "Failed to execute GET request to Kapacitor REST API (#{$!})"
      end

      data
    end

    # Perform a HTTP POST request
    #
    # @param endpoint [String] HTTP API endpoint
    # @param data [Hash] Request data
    #
    def api_post(endpoint:, data:)
      begin
        resp = self.http.post([self.url, endpoint].join('/'), data.to_json, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        begin
          data = JSON.parse(resp.body) unless resp.body.empty?
        rescue JSON::ParserError
          raise Exception, "Failed to decode response message"
        end
        if resp.status != 200
          error = data.include?('error') ? data['error'] : data.inspect if data
          raise Exception, "Query returned a non successful HTTP code (Status: #{resp.status}, Reason: #{resp.reason}#{", Error: #{error}" if error}"
        end
      rescue
        raise Exception, "Failed to execute POST request to Kapacitor REST API (#{$!})"
      end

      data
    end

    # Perform a HTTP DELETE request
    #
    # @param endpoint [String] HTTP API endpoint
    #
    def api_delete(endpoint:)
      begin
        resp = self.http.delete([self.url, endpoint].join('/'), {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        begin
          data = JSON.parse(resp.body) unless resp.body.empty?
        rescue JSON::ParserError
          raise Exception, "Failed to decode response message"
        end
        if resp.status != 204
          error = data.include?('error') ? data['error'] : data.inspect if data
          raise Exception, "Query returned a non successful HTTP code (Status: #{resp.status}, Reason: #{resp.reason}#{", Error: #{error}" if error}"
        end
      rescue
        raise Exception, "Failed to execute DELETE request to Kapacitor REST API (#{$!})"
      end

      data
    end

    # Perform a HTTP PATCH request
    #
    # @param endpoint [String] HTTP API endpoint
    # @param data [Hash] Request data
    #
    def api_patch(endpoint:, data:)
      begin
        resp = self.http.patch([self.url, endpoint].join('/'), data.to_json, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        begin
          data = JSON.parse(resp.body) unless resp.body.empty?
        rescue JSON::ParserError
          raise Exception, "Failed to decode response message"
        end
        if resp.status != 200
          error = data.include?('error') ? data['error'] : data.inspect if data
          raise Exception, "Query returned a non successful HTTP code (Status: #{resp.status}, Reason: #{resp.reason}#{", Error: #{error}" if error}"
        end
      rescue
        raise Exception, "Failed to execute PATCH request to Kapacitor REST API (#{$!})"
      end

      data
    end

    # Perform a HTTP PUT request
    #
    # @param endpoint [String] HTTP API endpoint
    # @param data [Hash] Request data
    #
    def api_put(endpoint:, data:)
      begin
        resp = self.http.put([self.url, endpoint].join('/'), data.to_json, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        begin
          data = JSON.parse(resp.body) unless resp.body.empty?
        rescue JSON::ParserError
          raise Exception, "Failed to decode response message"
        end
        if resp.status != 200
          error = data.include?('error') ? data['error'] : data.inspect if data
          raise Exception, "Query returned a non successful HTTP code (Status: #{resp.status}, Reason: #{resp.reason}#{", Error: #{error}" if error}"
        end
      rescue
        raise Exception, "Failed to execute PUT request to Kapacitor REST API (#{$!})"
      end

      data
    end
  end
end
