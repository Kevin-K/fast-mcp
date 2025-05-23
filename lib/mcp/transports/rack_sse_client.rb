# frozen_string_literal: true

require 'json'
require 'securerandom'

module FastMcp
  module Transports
    # Represents a single SSE client connection in the RackTransport
    class RackSseClient
      attr_reader :id, :stream, :connected_at, :mutex

      def initialize(id, stream, logger, transport)
        @id = id
        @stream = stream
        @connected_at = Time.now
        @mutex = Mutex.new
        @logger = logger
        @transport = transport
        @closed = false
        @ping_count = 0
        @keep_alive_thread = nil
        start_keep_alive_thread
      end

      def write(message)
        return if closed?

        mutex.synchronize do
          json_message = message.is_a?(String) ? message : JSON.generate(message)
          stream.write("data: #{json_message}\n\n")
          stream.flush if stream.respond_to?(:flush)
        end
      rescue Errno::EPIPE, IOError => e
        @logger.info("Client #{id} disconnected: #{e.message}")
        close
        raise
      rescue StandardError => e
        @logger.error("Error sending message to client #{id}: #{e.message}")
        close
        raise
      end

      def write_comment(comment)
        return if closed?

        mutex.synchronize do
          stream.write(": #{comment}\n\n")
          stream.flush if stream.respond_to?(:flush)
        end
      rescue Errno::EPIPE, IOError => e
        @logger.info("Client #{id} disconnected: #{e.message}")
        close
        raise
      rescue StandardError => e
        @logger.error("Error sending comment to client #{id}: #{e.message}")
        close
        raise
      end

      def write_event(event_name, data)
        return if closed?

        mutex.synchronize do
          json_data = data.is_a?(String) ? data : JSON.generate(data)
          stream.write("event: #{event_name}\ndata: #{json_data}\n\n")
          stream.flush if stream.respond_to?(:flush)
        end
      rescue Errno::EPIPE, IOError => e
        @logger.info("Client #{id} disconnected: #{e.message}")
        close
        raise
      rescue StandardError => e
        @logger.error("Error sending event to client #{id}: #{e.message}")
        close
        raise
      end

      def send_keep_alive_ping
        return if closed?

        @ping_count += 1
        # Send a comment before each ping to keep the connection alive
        write_comment("keep-alive #{@ping_count}")
        # Only send actual ping events every 5 counts to reduce overhead
        if (@ping_count % 5).zero?
          @logger.debug("Sending ping ##{@ping_count} to SSE client #{id}")
          send_ping_event
        end
      end

      def send_ping_event
        return if closed?

        ping_message = {
          jsonrpc: '2.0',
          method: 'ping',
          id: rand(1_000_000)
        }
        write_event("message", ping_message)
      end

      def close
        return if closed?

        mutex.synchronize do
          stop_keep_alive_thread
          stream.close if stream.respond_to?(:close) && !stream.closed?
          @closed = true
        end
      rescue StandardError => e
        @logger.error("Error closing client #{id}: #{e.message}")
      end

      def closed?
        @closed || (stream.respond_to?(:closed?) && stream.closed?)
      end

      private

      def start_keep_alive_thread
        @logger.info("Starting keep-alive thread for client #{id}")
        @keep_alive_thread = Thread.new do
          @logger.info("Keep-alive thread started for client #{id}")
          keep_alive_loop
        rescue StandardError => e
          @logger.error("Error in SSE keep-alive for client #{id}: #{e.message}")
          @logger.error(e.backtrace.join("\n")) if e.backtrace
        ensure
          @logger.info("Keep-alive thread ending for client #{id}")
          @transport.unregister_sse_client(id)
        end
      end

      def stop_keep_alive_thread
        return unless @keep_alive_thread&.alive?

        @keep_alive_thread.exit
        @keep_alive_thread = nil
      end

      def keep_alive_loop
        @logger.info("Starting keep-alive loop for SSE connection #{id}")
        ping_interval = 1 # Send a ping every 1 second
        while @transport.running? && !closed?
          begin
            send_keep_alive_ping
            sleep ping_interval
          rescue Errno::EPIPE, IOError => e
            @logger.error("SSE connection error for client #{id}: #{e.message}")
            break
          end
        end
        @logger.info("Keep-alive loop ended for client #{id}. running: #{@transport.running?}, io_closed: #{closed?}")
      end
    end
  end
end