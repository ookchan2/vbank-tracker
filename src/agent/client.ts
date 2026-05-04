/**
 * Agent SDK client wrapper — V1 query() with structured output support.
 * Includes AI-unavailability flag for Copy3's graceful degradation.
 * @author Alfie
 */

import { query, type Query, type SDKMessage, type SDKResultMessage } from '@anthropic-ai/claude-agent-sdk';

export interface AgentQueryOptions {
  prompt: string;
  systemPromptAppend?: string;
  model?: string;
  maxTurns?: number;
  allowedTools?: string[];
  permissionMode?: 'default' | 'acceptEdits' | 'bypassPermissions' | 'plan' | 'dontAsk' | 'auto';
  outputSchema?: Record<string, any>;
  maxBudgetUsd?: number;
  cwd?: string;
  agents?: Record<string, any>;
  /** Called on each assistant turn — use for live progress feedback */
  onProgress?: (turn: number) => void;
}

export interface AgentResult {
  success: boolean;
  result?: string;
  structuredOutput?: any;
  costUsd?: number;
  durationMs?: number;
  numTurns?: number;
  errors?: string[];
  /** True if the Agent SDK was completely unavailable (not just a query error) */
  agentUnavailable?: boolean;
}

/** Track whether Agent SDK is available for the session */
let _agentAvailable = true;

/** Check if Agent SDK was previously detected as unavailable */
export function isAgentUnavailable(): boolean {
  return !_agentAvailable;
}

/**
 * Run an agent query and collect the final result.
 * Breaks out of the message stream as soon as a result message is received
 * to avoid capturing the subprocess exit error that follows.
 * @author Alfie
 */
export async function runAgent(opts: AgentQueryOptions): Promise<AgentResult> {
  if (!_agentAvailable) {
    return { success: false, agentUnavailable: true, errors: ['Agent SDK unavailable (previous init failed)'] };
  }

  const options: any = {
    model: opts.model || 'glm-5',
    maxTurns: opts.maxTurns || 30,
    permissionMode: opts.permissionMode || 'bypassPermissions',
    allowDangerouslySkipPermissions: opts.permissionMode === 'bypassPermissions',
    allowedTools: opts.allowedTools || [],
    systemPrompt: opts.systemPromptAppend
      ? { type: 'preset' as const, preset: 'claude_code' as const, append: opts.systemPromptAppend }
      : undefined,
    settingSources: ['user', 'project'] as any,
  };

  if (opts.outputSchema) {
    options.outputFormat = {
      type: 'json_schema',
      schema: opts.outputSchema,
    };
  }

  if (opts.maxBudgetUsd) {
    options.maxBudgetUsd = opts.maxBudgetUsd;
  }

  if (opts.cwd) {
    options.cwd = opts.cwd;
  }

  if (opts.agents) {
    options.agents = opts.agents;
  }

  let lastResult: SDKResultMessage | null = null;
  let turnCount = 0;

  try {
    const q: Query = query({ prompt: opts.prompt, options });

    for await (const message of q) {
      if (message.type === 'assistant') {
        turnCount++;
        opts.onProgress?.(turnCount);
      }
      if (message.type === 'result') {
        lastResult = message as SDKResultMessage;
        break;
      }
    }

    if (!lastResult) {
      return { success: false, errors: ['No result message received from agent'] };
    }

    if (lastResult.subtype === 'success') {
      return {
        success: true,
        result: lastResult.result,
        structuredOutput: (lastResult as any).structured_output,
        costUsd: lastResult.total_cost_usd,
        durationMs: lastResult.duration_ms,
        numTurns: lastResult.num_turns,
      };
    } else {
      return {
        success: false,
        errors: (lastResult as any).errors || [`Agent error: ${lastResult.subtype}`],
        costUsd: lastResult.total_cost_usd,
        durationMs: lastResult.duration_ms,
        numTurns: lastResult.num_turns,
      };
    }
  } catch (err: any) {
    // Detect SDK init failure — mark as unavailable for remainder of session
    if (/cannot find module|not found|ECONNREFUSED|spawn/i.test(err.message || '')) {
      _agentAvailable = false;
      return { success: false, agentUnavailable: true, errors: [`Agent SDK unavailable: ${err.message}`] };
    }

    // If we already captured a result before the subprocess exit, return it
    if (lastResult) {
      if (lastResult.subtype === 'success') {
        return {
          success: true,
          result: lastResult.result,
          structuredOutput: (lastResult as any).structured_output,
          costUsd: lastResult.total_cost_usd,
          durationMs: lastResult.duration_ms,
          numTurns: lastResult.num_turns,
        };
      }
      return {
        success: false,
        errors: (lastResult as any).errors || [`Agent error: ${lastResult.subtype}`],
        costUsd: lastResult.total_cost_usd,
        durationMs: lastResult.duration_ms,
        numTurns: lastResult.num_turns,
      };
    }
    return { success: false, errors: [err.message || String(err)] };
  }
}

/**
 * Pre-warm an agent subprocess for faster first query.
 * @author Alfie
 */
export async function warmup(): Promise<void> {
  try {
    const sdk = await import('@anthropic-ai/claude-agent-sdk');
    const startupFn = (sdk as any).startup;
    if (typeof startupFn !== 'function') {
      console.log('  ⚠️  Agent warmup skipped (startup not available in this SDK version)');
      return;
    }
    const warm = await startupFn({
      options: {
        model: 'glm-5',
        permissionMode: 'bypassPermissions',
        allowDangerouslySkipPermissions: true,
      } as any,
      initializeTimeoutMs: 30000,
    });
    warm.close();
    console.log('  🔥 Agent subprocess warmed up');
  } catch {
    _agentAvailable = false;
    console.log('  ⚠️  Agent warmup failed — AI features will be unavailable');
  }
}
