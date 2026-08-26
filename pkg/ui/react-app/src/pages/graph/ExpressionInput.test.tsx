import * as React from 'react';
import { mount, ReactWrapper } from 'enzyme';
import { EditorState } from '@codemirror/state';
import { CompletionContext, CompletionResult } from '@codemirror/autocomplete';
import { CompleteStrategy } from '@prometheus-io/codemirror-promql';
import ExpressionInput, { clampCompletionResult, HistoryCompleteStrategy } from './ExpressionInput';
import { Button, InputGroup, InputGroupAddon } from 'reactstrap';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch, faSpinner } from '@fortawesome/free-solid-svg-icons';

describe('ExpressionInput', () => {
  const expressionInputProps = {
    value: 'node_cpu',
    queryHistory: [],
    metricNames: [],
    executeQuery: (): void => {
      // Do nothing.
    },
    onExpressionChange: (): void => {
      // Do nothing.
    },
    loading: false,
    enableAutocomplete: true,
    enableHighlighting: true,
    enableLinter: true,
    executeExplain: (): void => {
      // Do nothing.
    },
    disableExplain: false,
  };

  let expressionInput: ReactWrapper;
  beforeEach(() => {
    expressionInput = mount(<ExpressionInput {...expressionInputProps} />);
  });

  it('renders an InputGroup', () => {
    const inputGroup = expressionInput.find(InputGroup);
    expect(inputGroup.prop('className')).toEqual('expression-input');
  });

  it('renders a search icon when it is not loading', () => {
    const addon = expressionInput.find(InputGroupAddon).filterWhere((addon) => addon.prop('addonType') === 'prepend');
    const icon = addon.find(FontAwesomeIcon);
    expect(icon.prop('icon')).toEqual(faSearch);
  });

  it('renders a loading icon when it is loading', () => {
    const expressionInput = mount(<ExpressionInput {...expressionInputProps} loading={true} />);
    const addon = expressionInput.find(InputGroupAddon).filterWhere((addon) => addon.prop('addonType') === 'prepend');
    const icon = addon.find(FontAwesomeIcon);
    expect(icon.prop('icon')).toEqual(faSpinner);
    expect(icon.prop('spin')).toBe(true);
  });

  it('renders a CodeMirror expression input', () => {
    const input = expressionInput.find('div.cm-expression-input');
    expect(input.text()).toContain('node_cpu');
  });

  it('renders an execute button', () => {
    const addon = expressionInput.find(InputGroupAddon).filterWhere((addon) => addon.prop('addonType') === 'append');
    const button = addon.find(Button).find('.execute-btn').first();
    expect(button.prop('color')).toEqual('primary');
    expect(button.text()).toEqual('Execute');
  });

  it('executes the query when clicking the execute button', () => {
    const spyExecuteQuery = jest.fn();
    const props = { ...expressionInputProps, executeQuery: spyExecuteQuery };
    const wrapper = mount(<ExpressionInput {...props} />);
    const btn = wrapper.find(Button).filterWhere((btn) => btn.hasClass('execute-btn'));
    btn.simulate('click');
    expect(spyExecuteQuery).toHaveBeenCalledTimes(1);
  });
});

// Regression tests for https://github.com/thanos-io/thanos/issues/8232 —
// queries becoming uneditable after cut/paste. The editor freezes because a
// completion result whose range points past the end of the (shortened)
// document gets stored in CodeMirror's autocomplete state; every following
// transaction then maps that stale range via ChangeSet.mapPos and throws
// "Position X is out of range for changeset of length Y". We guard against it
// by clamping every completion range to the current document bounds before it
// is handed to CodeMirror.
describe('clampCompletionResult', () => {
  it('passes null through unchanged', () => {
    expect(clampCompletionResult(null, 8)).toBeNull();
  });

  it('leaves an in-range result untouched (same reference)', () => {
    const result: CompletionResult = { from: 0, to: 8, options: [] };
    expect(clampCompletionResult(result, 8)).toBe(result);
  });

  it('clamps a `to` that points beyond the document', () => {
    const clamped = clampCompletionResult({ from: 0, to: 65536, options: [] }, 8);
    expect(clamped).toEqual({ from: 0, to: 8, options: [] });
  });

  it('clamps a `from` that points beyond the document', () => {
    const clamped = clampCompletionResult({ from: 65536, to: 65536, options: [] }, 8);
    // `from` is clamped to the doc length and `to` is never left below `from`.
    expect(clamped).toMatchObject({ from: 8, to: 8 });
  });

  it('preserves an undefined `to`', () => {
    const clamped = clampCompletionResult({ from: 65536, options: [] }, 8);
    expect(clamped).toMatchObject({ from: 8, to: undefined });
  });
});

describe('HistoryCompleteStrategy range clamping (#8232)', () => {
  // A CompletionContext whose document is only 8 characters long.
  const contextFor = (doc: string): CompletionContext =>
    new CompletionContext(EditorState.create({ doc }), doc.length, false);

  // Stub inner strategy that returns a range past the end of the document,
  // mimicking the out-of-range positions observed in the bug report.
  const outOfRangeStrategy = (result: CompletionResult | null): CompleteStrategy => ({
    promQL: () => result,
  });

  it('never returns a completion range past the end of the document', async () => {
    const strategy = new HistoryCompleteStrategy(outOfRangeStrategy({ from: 3, to: 65536, options: [] }), []);
    const context = contextFor(' == true'); // length 8, matches the bug report

    const res = await strategy.promQL(context);
    expect(res).not.toBeNull();
    const result = res as CompletionResult;
    expect(result.from).toBeLessThanOrEqual(context.state.doc.length);
    expect(result.to ?? 0).toBeLessThanOrEqual(context.state.doc.length);
    expect(result.to).toBe(8);
  });

  it('clamps the enriched history completion range as well', async () => {
    // start === 0 branch: the history items are merged in and must be clamped too.
    const strategy = new HistoryCompleteStrategy(outOfRangeStrategy({ from: 0, to: 65536, options: [] }), [
      'up',
      'rate(x[5m])',
    ]);
    const context = contextFor(' == true');

    const res = await strategy.promQL(context);
    const result = res as CompletionResult;
    expect(result.from).toBe(0);
    expect(result.to).toBeLessThanOrEqual(context.state.doc.length);
    // history queries are still offered
    expect(result.options.length).toBeGreaterThanOrEqual(2);
  });

  it('never produces an out-of-range range even with a null inner result', async () => {
    const strategy = new HistoryCompleteStrategy(outOfRangeStrategy(null), ['up']);
    const context = contextFor(' == true');
    const res = await strategy.promQL(context);
    if (res !== null) {
      expect(res.from).toBeLessThanOrEqual(context.state.doc.length);
      expect(res.to ?? 0).toBeLessThanOrEqual(context.state.doc.length);
    }
  });
});
