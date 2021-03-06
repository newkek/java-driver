/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * A message from the CQL binary protocol.
 */
abstract class Message {

    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Coder<R extends Request> {
        public void encode(R request, ByteBuf dest);
        public int encodedSize(R request);
    }

    public interface Decoder<R extends Response> {
        public R decode(ByteBuf body);
    }

    private volatile int streamId;

    protected Message() {}

    public Message setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId() {
        return streamId;
    }

    public static abstract class Request extends Message {

        public enum Type {
            STARTUP        (1, Requests.Startup.coder, Requests.Startup.coder),
            CREDENTIALS    (4, Requests.Credentials.coder, null),
            OPTIONS        (5, Requests.Options.coder, Requests.Options.coder),
            QUERY          (7, Requests.Query.coderV1, Requests.Query.coderV2),
            PREPARE        (9, Requests.Prepare.coder, Requests.Prepare.coder),
            EXECUTE        (10, Requests.Execute.coderV1, Requests.Execute.coderV2),
            REGISTER       (11, Requests.Register.coder, Requests.Register.coder),
            BATCH          (13, null, Requests.Batch.coder),
            AUTH_RESPONSE  (15, Requests.AuthResponse.coder, Requests.AuthResponse.coder);

            public final int opcode;
            private final Coder<?> coderV1;
            private final Coder<?> coderV2;

            private Type(int opcode, Coder<?> coderV1, Coder<?> coderV2) {
                this.opcode = opcode;
                this.coderV1 = coderV1;
                this.coderV2 = coderV2;
            }

            public Coder<?> coder(int version)
            {
                assert version == 1 || version == 2 : "Unsupported protocol version, we shouldn't have arrived here";
                return version == 1 ? coderV1 : coderV2;
            }
        }

        public final Type type;
        protected boolean tracingRequested;

        protected Request(Type type) {
            this.type = type;
        }

        public void setTracingRequested() {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested() {
            return tracingRequested;
        }

        ConsistencyLevel consistency() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.consistency;
                case EXECUTE: return ((Requests.Execute)this).options.consistency;
                case BATCH:   return ((Requests.Batch)this).consistency;
                default:      return null;
            }
        }

        ConsistencyLevel serialConsistency() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.serialConsistency;
                case EXECUTE: return ((Requests.Execute)this).options.serialConsistency;
                default:      return null;
            }
        }

        ByteBuffer pagingState() {
            switch (this.type) {
                case QUERY:   return ((Requests.Query)this).options.pagingState;
                case EXECUTE: return ((Requests.Execute)this).options.pagingState;
                default:      return null;
            }
        }
    }

    public static abstract class Response extends Message {

        public enum Type {
            ERROR          (0, Responses.Error.decoder, Responses.Error.decoder),
            READY          (2, Responses.Ready.decoder, Responses.Ready.decoder),
            AUTHENTICATE   (3, Responses.Authenticate.decoder, Responses.Authenticate.decoder),
            SUPPORTED      (6, Responses.Supported.decoder, Responses.Supported.decoder),
            RESULT         (8, Responses.Result.decoderV1, Responses.Result.decoderV2),
            EVENT          (12, Responses.Event.decoder, Responses.Event.decoder),
            AUTH_CHALLENGE (14, Responses.AuthChallenge.decoder, Responses.AuthChallenge.decoder),
            AUTH_SUCCESS   (16, Responses.AuthSuccess.decoder, Responses.AuthSuccess.decoder);

            public final int opcode;
            private final Decoder<?> decoderV1;
            private final Decoder<?> decoderV2;

            private static final Type[] opcodeIdx;
            static {
                int maxOpcode = -1;
                for (Type type : Type.values())
                    maxOpcode = Math.max(maxOpcode, type.opcode);
                opcodeIdx = new Type[maxOpcode + 1];
                for (Type type : Type.values()) {
                    if (opcodeIdx[type.opcode] != null)
                        throw new IllegalStateException("Duplicate opcode");
                    opcodeIdx[type.opcode] = type;
                }
            }

            private Type(int opcode, Decoder<?> decoderV1, Decoder<?> decoderV2) {
                this.opcode = opcode;
                this.decoderV1 = decoderV1;
                this.decoderV2 = decoderV2;
            }

            public static Type fromOpcode(int opcode) {
                Type t = opcodeIdx[opcode];
                if (t == null)
                    throw new DriverInternalError(String.format("Unknown response opcode %d", opcode));
                return t;
            }

            public Decoder<?> decoder(int version)
            {
                assert version == 1 || version == 2 : "Unsupported protocol version, we shouldn't have arrived here";
                return version == 1 ? decoderV1 : decoderV2;
            }
        }

        public final Type type;
        protected UUID tracingId;

        protected Response(Type type) {
            this.type = type;
        }

        public Response setTracingId(UUID tracingId) {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId() {
            return tracingId;
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {

        @Override
        protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);
            UUID tracingId = isTracing ? CBUtil.readUUID(frame.body) : null;

            try {
                Response response = Response.Type.fromOpcode(frame.header.opcode).decoder(frame.header.version).decode(frame.body);
                response.setTracingId(tracingId).setStreamId(frame.header.streamId);
                out.add(response);
            } finally {
                frame.body.release();
            }
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Request> {

        private final int protocolVersion;

        public ProtocolEncoder(int version) {
            this.protocolVersion = version;
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Request request, List<Object> out) throws Exception {
            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);
            if (request.isTracingRequested())
                flags.add(Frame.Header.Flag.TRACING);

            @SuppressWarnings("unchecked")
            Coder<Request> coder = (Coder<Request>)request.type.coder(protocolVersion);
            ByteBuf body = ctx.alloc().buffer(coder.encodedSize(request));
            coder.encode(request, body);

            out.add(Frame.create(protocolVersion, request.type.opcode, request.getStreamId(), flags, body));
        }
    }
}
