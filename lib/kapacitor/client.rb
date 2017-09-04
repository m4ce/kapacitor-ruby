require 'net/http'
require 'json'

module Kapacitor
  class Client
    # @return [URI] Kapacitor REST API URL
    attr_reader :uri
    # @return [Net::HTTP] HTTP client instance
    attr_reader :http

    # Create a new client
    #
    # @param url [String] Kapacitor REST API's URL (defaults to `http://localhost:9092`)
    # @param version [Integer] API version (defaults to `v1preview`)
    #
    def initialize(host: 'http://localhost:9092', version: 'v1preview')
      @uri = URI.parse("#{host}/kapacitor/#{version}")
      @http = Net::HTTP.new(@uri.host, @uri.port)
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

      api_post(endpoint: '/templates', data: req)
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

      api_patch(endpoint: "/templates/#{id}", data: req) unless req.empty?
    end

    # Delete a Kapacitor template
    #
    # @param id [String] Template ID
    #
    def delete_template(id:)
      api_delete(endpoint: "/templates/#{id}")
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
        res = api_get(endpoint: "/templates?offset=#{offset}&limit=#{limit}")['templates']
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
      elsif opts['template_id'].nil? && (opts['type'].nil? || opts['script'].nil?)
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

      api_post(endpoint: '/tasks', data: req)
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
      req['status'] = opts[:status] if opts[:status]
      req['vars'] = opts[:vars] if opts[:vars]

      if opts[:type]
        raise ArgumentError, "Kapacitor template type can be either 'batch' or 'stream'" unless opts[:type] == 'batch' || opts[:type] == 'stream'
      end

      if opts['status']
        raise ArgumentError, "Kapacitor task status can be either 'enabled' or 'disabled'" unless opts[:status] == 'enabled' || opts[:status] == 'disabled'
      end

      api_patch(endpoint: "/tasks/#{id}", data: req) unless req.empty?
    end

    # Delete a Kapacitor task
    #
    # @param id [String] Task ID
    #
    def delete_task(id:)
      api_delete(endpoint: "/tasks/#{id}")
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
        res = api_get(endpoint: "/tasks?fields=id&offset=#{offset}&limit=#{limit}")['tasks']
        break unless res.size > 0

        res.each do |task|
          tasks << api_get(endpoint: "/tasks/#{task['id']}")
        end

        offset += limit
      end

      tasks
    end

    # Define a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    # @param actions [Array[Hash]] Handler actions
    #
    def define_topic_handler(id:, topic:, actions:)
      req = {}
      req['id'] = id

      actions = [actions] unless actions.is_a?(Array)
      raise ArgumentError, "Kapacitor topic handler requires one or more actions" unless actions.size > 0

      actions.each do |action|
        raise ArgumentError, "Missing required kind attribute for action #{action}"
      end

      req['actions'] = actions
      api_post(endpoint: "/alerts/topics/#{topic}/handlers", data: req)
    end

    # Update a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    # @param actions [Array[Hash]] Handler actions
    #
    def update_topic_handler(id:, topic:, actions:)
      req = {}

      actions = [actions] unless actions.is_a?(Array)
      raise ArgumentError, "Kapacitor topic handler requires one or more actions" unless actions.size > 0

      req['actions'] = actions
      api_put(endpoint: "/alerts/topics/#{topic}/handlers/#{id}", data: req) unless req.empty?
    end

    # Delete a topic handler
    #
    # @param id [String] Handler ID
    # @param topic [String] Topic name
    #
    def delete_topic_handler(id:, topic:)
      api_delete(endpoint: "/alerts/topics/#{topic}/handlers/#{id}")
    end

    # Retrieve topic's handlers
    #
    # @param topic [String] Topic name
    # @return [Array[Hash]] List of handlers
    #
    def topic_handlers(topic:)
      return api_get(endpoint: "/alerts/topics/#{topic}/handlers")['handlers']
    end

private
    # Perform a HTTP GET request
    #
    # @param endpoint [String] HTTP API endpoint
    # @return [Array[Hash], Hash] API response
    #
    def api_get(endpoint:)
      begin
        req = Net::HTTP::Get.new(self.uri.path + endpoint, {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
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
        req = Net::HTTP::Post.new(self.uri.path + endpoint, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        req.body = data.to_json
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
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
        req = Net::HTTP::Delete.new(self.uri.path + endpoint, {'Content-type' => 'application/json', 'Accept' => 'application/json'})
        resp = self.http.request(req)

        if resp.code == '204'
          if resp.body
            begin
              data = JSON.parse(resp.body)
            rescue JSON::ParserError
              raise Exception, "Failed to decode response message"
            end
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
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
        req = Net::HTTP::Patch.new(self.uri.path + endpoint, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        req.body = data.to_json
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
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
        req = Net::HTTP::Put.new(self.uri.path + endpoint, {'Content-Type' => 'application/json', 'Accept' => 'application/json'})
        req.body = data.to_json
        resp = self.http.request(req)

        if resp.code == '200'
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute PUT request to Kapacitor REST API (#{$!})"
      end

      data
    end
  end
end
