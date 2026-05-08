/**
 * Poe API client for AI extraction.
 * Uses Claude-3.5-Sonnet via Poe API for best balance of quality and speed.
 * Note: Poe uses simplified model names like 'claude-sonnet-3.7' instead of full Anthropic IDs.
 * @author Claude
 */

import { generateText } from 'ai';
import { poe } from 'ai-sdk-provider-poe';

export interface DirectAIResult {
  success: boolean;
  result?: string;
  structuredOutput?: any;
  costUsd?: number;
  durationMs?: number;
  errors?: string[];
}

/**
 * Call Poe API with Claude-3.5-Sonnet for structured extraction.
 * Uses 'claude-sonnet-3.7' which is the closest to Claude-3.5-Sonnet on Poe.
 */
export async function callAnthropicDirect(
  prompt: string,
  systemPrompt: string,
  outputSchema: Record<string, any>,
  maxTokens: number = 4096,
): Promise<DirectAIResult> {
  const t0 = Date.now();

  try {
    const { text, usage } = await generateText({
      model: poe('claude-sonnet-3.7'),
      maxOutputTokens: maxTokens,
      temperature: 0.1,
      system: systemPrompt,
      prompt,
    });

    // Calculate approximate cost (Claude 3.5 Sonnet pricing via Poe)
    // Input: ~$3/1M tokens, Output: ~$15/1M tokens
    const inputTokens = usage.inputTokens ?? 0;
    const outputTokens = usage.outputTokens ?? 0;
    const costUsd = (inputTokens / 1_000_000 * 3 + outputTokens / 1_000_000 * 15);

    // Try to parse as JSON
    let structuredOutput = null;
    try {
      // Try direct parse first
      structuredOutput = JSON.parse(text);
    } catch {
      // Try to extract JSON from markdown code blocks
      const jsonMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (jsonMatch) {
        try {
          structuredOutput = JSON.parse(jsonMatch[1].trim());
        } catch {
          // Try to find raw JSON array/object
          const arrayMatch = text.match(/\[[\s\S]*\]/);
          const objectMatch = text.match(/\{[\s\S]*\}/);
          if (arrayMatch) {
            structuredOutput = { promotions: JSON.parse(arrayMatch[0]) };
          } else if (objectMatch) {
            structuredOutput = JSON.parse(objectMatch[0]);
          }
        }
      }
    }

    return {
      success: true,
      result: text,
      structuredOutput,
      costUsd,
      durationMs: Date.now() - t0,
    };
  } catch (err: any) {
    return {
      success: false,
      errors: [err.message || String(err)],
      durationMs: Date.now() - t0,
    };
  }
}

/**
 * Check if running in an environment where Poe API should be used.
 * Requires POE_API_KEY to be set.
 */
export function shouldUseDirectAPI(): boolean {
  // If POE_API_KEY is set, use Poe API
  if (process.env.POE_API_KEY) {
    return true;
  }
  return false;
}
