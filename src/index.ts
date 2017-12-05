import { ChildProcess, spawn } from 'child_process';
import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { EventEmitter } from 'events';
import { parse as urlParser } from 'url';

// Typings are not available
const StreamJsonObjects = require('stream-json/utils/StreamJsonObjects');

import {
    MiddlewareParams,
    makeMicroMiddleware,
    makeExpressMiddleware,
    makeConnectMiddleware,
    makeKoaMiddleware,
    instrumentHapi
} from './middleware';

export type LogLevels = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

export interface AccessLogConfig {
    destination: string,
    requestHeaders?: string[],
    responseHeaders?: string[],
}

export interface ExtensionsConfig {
    strip?: string[],
    blacklist?: string[],
}

// User-configurable fields of EngineConfig "frontend"
export interface FrontendParams {
    extensions?: ExtensionsConfig,
}

// All configuration of "frontend" (including fields managed by apollo-engine-js)
export interface FrontendConfig extends FrontendParams {
    host: string,
    endpoint: string,
    port: number,
}

// User-configurable fields of EngineConfig "origin"
export interface OriginParams {
    requestTimeout?: string,
    maxConcurrentRequests?: number,
    supportsBatch?: boolean,
}

// All configuration of "origin"  (including fields managed by apollo-engine-js)
export interface OriginConfig extends OriginParams {
    http: {
        url: string,
        headerSecret: string,
    }
}

export interface EngineConfig {
    apiKey: string,
    origins?: OriginConfig[],
    frontends?: FrontendConfig[],
    stores?: {
        name: string,
        memcache?: {
            url: string[],
            timeout?: string,
            keyPrefix?: string,
        },
        inMemory?: {
            cacheSize?: number
        },
    }[],
    sessionAuth?: {
        header?: string,
        cookie?: string,
        tokenAuthUrl?: string
        store?: string,
    },
    logging?: {
        level?: LogLevels,
        request?: AccessLogConfig,
        query?: AccessLogConfig,
        format?: string,
        destination?: string,
    },
    reporting?: {
        endpointUrl?: string,
        maxAttempts?: number,
        retryMinimum?: string,
        retryMaximum?: string,
        debugReports?: boolean,
        noTraceVariables?: boolean,
        privateHeaders?: string[],
        privateVariables?: string[],
        disabled?: boolean,
        proxyUrl?: string,
    },
    queryCache?: {
        publicFullQueryStore?: string,
        privateFullQueryStore?: string,
    }
}

export interface SideloadConfig {
    engineConfig: string | EngineConfig,
    endpoint?: string,
    graphqlPort?: number,
    dumpTraffic?: boolean,
    startupTimeout?: number,
    origin?: OriginParams,
    frontend?: FrontendParams,
}

export class Engine extends EventEmitter {
    private child: ChildProcess | null;
    private graphqlPort: number;
    private binary: string;
    private engineConfig: EngineConfig;
    private middlewareParams: MiddlewareParams;
    private running: boolean;
    private startupTimeout: number;
    private originParams: OriginParams;
    private frontendParams: FrontendParams;

    public constructor(sideloadConfig: SideloadConfig) {
        super();
        this.running = false;
        this.startupTimeout = sideloadConfig.startupTimeout || 1000;

        // Ensure that engineConfig is EngineConfig and not string
        let sourceConfig = sideloadConfig.engineConfig
        if (typeof sourceConfig === 'string') {
            sourceConfig = JSON.parse(readFileSync(sourceConfig as string, 'utf8') as string);
        }
        this.engineConfig = Object.assign({}, sourceConfig as EngineConfig);

        // Middleware configuration for double proxy
        this.middlewareParams = new MiddlewareParams();
        this.middlewareParams.endpoint = sideloadConfig.endpoint || '/graphql';
        this.middlewareParams.psk = randomBytes(48).toString("hex");
        this.middlewareParams.dumpTraffic = sideloadConfig.dumpTraffic || false;

        this.originParams = sideloadConfig.origin || {};
        this.frontendParams = sideloadConfig.frontend || {};

        if (sideloadConfig.graphqlPort) {
            this.graphqlPort = sideloadConfig.graphqlPort;
        } else {
            const port: any = process.env.PORT;
            if (isFinite(port)) {
                this.graphqlPort = parseInt(port, 10);
            } else {
                throw new Error(`Neither 'graphqlPort' nor process.env.PORT is set. ` +
                    `In order for Apollo Engine to act as a proxy for your GraphQL server, ` +
                    `it needs to know which port your GraphQL server is listening on (this is ` +
                    `the port number that comes before '/graphql'). If you see this error, you ` +
                    `should make sure to add e.g. 'graphqlPort: 1234' wherever you call new Engine(...).`);
            }
        }

        // Inject frontend, that we will route
        const frontend = Object.assign({}, this.frontendParams, {
            host: '127.0.0.1',
            endpoint: this.middlewareParams.endpoint,
            port: 0,
        });
        if (typeof this.engineConfig.frontends === 'undefined') {
            this.engineConfig.frontends = [frontend];
        } else {
            this.engineConfig.frontends.push(frontend);
        }

        if (typeof this.engineConfig.origins === 'undefined') {
            const origin = Object.assign({}, this.originParams, {
                http: {
                    url: 'http://127.0.0.1:' + this.graphqlPort + this.middlewareParams.endpoint,
                    headerSecret: this.middlewareParams.psk
                },
            });
            this.engineConfig.origins = [origin];
        } else {
            // Extend any existing HTTP origins with the chosen PSK:
            // (trust it to fill other fields correctly)
            this.engineConfig.origins.forEach(origin => {
                if (typeof origin.http === 'object') {
                    Object.assign(origin.http, {
                        headerSecret: this.middlewareParams.psk,
                    });
                }
            });
        }

        // Logging format _must_ be JSON to stdout
        if (!this.engineConfig.logging) {
            this.engineConfig.logging = {}
        } else {
            if (this.engineConfig.logging.format && this.engineConfig.logging.format !== 'JSON') {
                console.error(`Invalid logging format: ${this.engineConfig.logging.format}, overridden to JSON.`);
            }
            if (this.engineConfig.logging.destination && this.engineConfig.logging.destination !== 'STDOUT') {
                console.error(`Invalid logging destination: ${this.engineConfig.logging.format}, overridden to STDOUT.`);
            }
        }
        this.engineConfig.logging.format = 'JSON';
        this.engineConfig.logging.destination = 'STDOUT';

        switch (process.platform) {
            case 'darwin': {
                this.binary = require.resolve('apollo-engine-binary-darwin/engineproxy_darwin_amd64');
                break;
            }
            case 'linux': {
                this.binary = require.resolve('apollo-engine-binary-linux/engineproxy_linux_amd64');
                break;
            }
            case 'win32': {
                this.binary = require.resolve('apollo-engine-binary-windows/engineproxy_windows_amd64.exe');
                break;
            }
            default: {
                throw new Error('Unsupported platform');
            }
        }
    }

    public start(): Promise<number> {
        if (this.running) {
            throw new Error('Only call start() on an engine object once');
        }
        this.running = true;

        const spawnChild = () => {
            // If logging >INFO, still log at info, then filter in node:
            // This is because startup notifications are at INFO level.
            let logLevelFilter: any;
            const logLevel = this.engineConfig.logging!.level;
            if (logLevel) {
                if (logLevel.match(/^warn(ing)?$/i)) {
                    this.engineConfig.logging!.level = 'info';
                    logLevelFilter = /^(warn(ing)?|error|fatal)$/;
                } else if (logLevel.match(/^error$/i)) {
                    this.engineConfig.logging!.level = 'info';
                    logLevelFilter = /^(error|fatal)$/;
                } else if (logLevel.match(/^fatal$/i)) {
                    this.engineConfig.logging!.level = 'info';
                    logLevelFilter = /^fatal$/;
                }
            }
            let childConfigJson = JSON.stringify(this.engineConfig) + '\n';

            const child = spawn(this.binary, ['-config=stdin']);
            this.child = child;

            const logStream = StreamJsonObjects.make();
            logStream.output.on('data', (logData: any) => {
                const logRecord = logData.value;

                // Look for message indicating successful startup:
                if (logRecord.msg === 'Started HTTP server.') {
                    const address = logRecord.address;
                    this.middlewareParams.uri = `http://${address}`;

                    // Notify proxy has started:
                    this.emit('start');

                    // If we hacked the log level, revert:
                    if (logLevelFilter) {
                        this.engineConfig.logging!.level = logLevel;
                        childConfigJson = JSON.stringify(this.engineConfig) + '\n';
                        child.stdin.write(childConfigJson);

                        // Remove the filter after the child has had plenty of time to reload the config:
                        setTimeout(() => {
                            logLevelFilter = null;
                        }, 1000);
                    }
                }

                // Print log message:
                if (!logLevelFilter || !logRecord.level || logRecord.level.match(logLevelFilter)) {
                    console.log({ proxy: logRecord });
                }
            });

            logStream.input.on('error', () => {
                // We received non-json output, dump it to stderr:
                console.error(logStream.input._buffer);
            });
            // Connect log hooks:
            child.stdout.pipe(logStream.input);
            child.stderr.pipe(process.stderr);

            // Feed config into process:
            child.stdin.write(childConfigJson);

            // Connect shutdown hooks:
            child.on('exit', (code, signal) => {
                // Wipe the URI, so middleware doesn't route to dead process:
                this.middlewareParams.uri = '';

                if (!this.running) {
                    // It's not an error if we think it's our fault.
                    return;
                }
                if (code === 78) {
                    this.emit('error', new Error('Engine crashed due to invalid configuration.'));
                    return;
                }

                if (code != null) {
                    console.error(`Engine crashed unexpectedly with code: ${code}`);
                }
                if (signal != null) {
                    console.error(`Engine was killed unexpectedly by signal: ${signal}`);
                }
                spawnChild();
            });
        };

        spawnChild();

        return new Promise((resolve, reject) => {
            const cancelTimeout = setTimeout(() => {
                this.running = false;
                if (this.child) {
                    this.child.kill('SIGKILL');
                    this.child = null;
                }
                return reject(Error('timed out'));
            }, this.startupTimeout);

            this.on('start', () => {
                clearTimeout(cancelTimeout);
                const port = urlParser(this.middlewareParams.uri).port;
                if (!port) {
                    return reject('unknown url');
                }
                resolve(parseInt(port, 10));
            });
        });
    }

    public microMiddleware(): (fn: Function) => void {
        return makeMicroMiddleware(this.middlewareParams);
    }

    public expressMiddleware(): (req: any, res: any, next: any) => void {
        return makeExpressMiddleware(this.middlewareParams);
    }

    public connectMiddleware(): (req: any, res: any, next: any) => void {
        return makeConnectMiddleware(this.middlewareParams);
    }

    public koaMiddleware(): (ctx: any, next: any) => void {
        return makeKoaMiddleware(this.middlewareParams);
    }

    public instrumentHapiServer(server: any) {
        instrumentHapi(server, this.middlewareParams);
    }

    public stop(): Promise<void> {
        if (this.child === null) {
            throw new Error('No engine instance running...');
        }
        const childRef = this.child;
        this.child = null;
        this.running = false;
        return new Promise((resolve) => {
            childRef.on('exit', () => {
                resolve();
            });
            childRef.kill();
        });
    }
}
