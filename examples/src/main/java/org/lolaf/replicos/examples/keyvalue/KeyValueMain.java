/*
 * Copyright © 2024-2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.replicos.examples.keyvalue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Runs a key-value node from the command line, reading commands from the standard input.
 */
public final class KeyValueMain {

    private static final long EPOCH = 1;
    private static final String USAGE = "usage: leader <port> | follower <port> <leader host>:<leader port>";
    private static final String COMMANDS = "commands: put <key> <value>, remove <key>, get <key>, list, quit";

    private KeyValueMain() {
    }

    public static void main(String[] args) throws IOException {
        PrintStream out = System.out;
        if (args.length < 2 || !(args[0].equals("leader") || args[0].equals("follower") && args.length == 3)) {
            out.println(USAGE);
            System.exit(1);
        }
        try (KeyValueNode node = new KeyValueNode(args[0] + "-" + args[1], new InetSocketAddress(Integer.parseInt(args[1])))) {
            if (args[0].equals("leader")) {
                node.lead(EPOCH);
            } else {
                node.follow(parseAddress(args[2]), EPOCH);
            }
            out.println(COMMANDS);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null && !line.trim().equals("quit")) {
                out.println(run(node, line.trim().split("\\s+", 3)));
            }
        }
    }

    private static String run(KeyValueNode node, String[] command) {
        switch (command[0]) {
            case "put":
                return command.length == 3 ? write(node, command) : COMMANDS;
            case "remove":
                return command.length == 2 ? write(node, command) : COMMANDS;
            case "get":
                return command.length == 2 ? String.valueOf(node.get(command[1])) : COMMANDS;
            case "list":
                return node.entries().toString();
            default:
                return COMMANDS;
        }
    }

    private static String write(KeyValueNode node, String[] command) {
        if (!node.isLeading()) {
            return "only the leader writes";
        }
        return (command[0].equals("put") ? node.put(command[1], command[2]) : node.remove(command[1])).toString();
    }

    private static InetSocketAddress parseAddress(String hostAndPort) {
        int colon = hostAndPort.lastIndexOf(':');
        return new InetSocketAddress(hostAndPort.substring(0, colon), Integer.parseInt(hostAndPort.substring(colon + 1)));
    }
}
