/**
 * Direct Anthropic API client for AI extraction.
 * Used as fallback when Agent SDK is not available (e.g., in GitHub Actions).
 * @author Claude
 */

import Anthropic from '@anthropic-ai/sdk';

const client = new Anthropic();

export interface DirectAIResult {
  success: boolean;
  result?: string;
  structuredOutput?: any;
  costUsd?: number;
  durationMs?: number;
  errors?: string[];
}

/**
 * Call Anthropic API directly for structured extraction.
 */
export async function callAnthropicDirect(
  prompt: string,
  systemPrompt: string,
  outputSchema: Record<string, any>,
  maxTokens: number = 4096,
): Promise<DirectAIResult> {
  const t0 = Date.now();

  try {
    const response = await client.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: maxTokens,
      temperature: 0.1,
      system: systemPrompt,
      messages: [
        {
          role: 'user',
          content: prompt,
        },
      ],
    });

    const textBlock = response.content.find(b => b.type === 'text');
    if (!textBlock || textBlock.type !== 'text') {
      return { success: false, errors: ['No text response from API'] };
    }

    const resultText = textBlock.text;

    // Calculate approximate cost (Claude Sonnet 4 pricing)
    const inputTokens = response.usage.input_tokens;
    const outputTokens = response.usage.output_tokens;
    const costUsd = (inputTokens / 1_000_000 * 3 + outputTokens / 1_000_000 * 15);

    // Try to parse as JSON
    let structuredOutput = null;
    try {
      // Try direct parse first
      structuredOutput = JSON.parse(resultText);
    } catch {
      // Try to extract JSON from markdown code blocks
      const jsonMatch = resultText.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (jsonMatch) {
        try {
          structuredOutput = JSON.parse(jsonMatch[1].trim());
        } catch {
          // Try to find raw JSON array/object
          const arrayMatch = resultText.match(/\[[\s\S]*\]/);
          const objectMatch = resultText.match(/\{[\s\S]*\}/);
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
      result: resultText,
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
 * Check if running in an environment where Agent SDK works.
 * Agent SDK requires Claude Code CLI to be installed.
 */
export function shouldUseDirectAPI(): boolean {
  // In GitHub Actions, Agent SDK doesn't work
  if (process.env.GITHUB_ACTIONS === 'true') {
    return true;
  }
  // If ANTHROPIC_API_KEY is set, prefer direct API
  if (process.env.ANTHROPIC_API_KEY) {
    return true;
  }
  return false;
}
