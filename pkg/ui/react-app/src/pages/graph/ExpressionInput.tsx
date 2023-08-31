import React, { FC, useEffect, useRef } from 'react';
import { Button, InputGroup, InputGroupAddon, InputGroupText } from 'reactstrap';
import { EditorView, highlightSpecialChars, keymap, ViewUpdate, placeholder } from '@codemirror/view';
import { EditorState, Prec, Compartment } from '@codemirror/state';
import { bracketMatching, indentOnInput, syntaxHighlighting, syntaxTree } from '@codemirror/language';
import { defaultKeymap, historyKeymap, history, insertNewlineAndIndent } from '@codemirror/commands';
import { highlightSelectionMatches } from '@codemirror/search';
import { lintKeymap } from '@codemirror/lint';
import { PromQLExtension, CompleteStrategy, newCompleteStrategy } from '@prometheus-io/codemirror-promql';
import {
  autocompletion,
  completionKeymap,
  CompletionContext,
  CompletionResult,
  closeBracketsKeymap,
  closeBrackets,
} from '@codemirror/autocomplete';
import { baseTheme, lightTheme, darkTheme, promqlHighlighter } from './CMTheme';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch, faSpinner } from '@fortawesome/free-solid-svg-icons';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useTheme } from '../../contexts/ThemeContext';

const promqlExtension = new PromQLExtension();

interface CMExpressionInputProps {
  value: string;
  onExpressionChange: (expr: string) => void;
  queryHistory: string[];
  metricNames: string[];
  executeQuery: () => void;
  loading: boolean;
  enableAutocomplete: boolean;
  enableHighlighting: boolean;
  enableLinter: boolean;
  executeExplain: () => void;
  disableExplain: boolean;
}

const dynamicConfigCompartment = new Compartment();

export interface ExplainTree {
  name: string;
  children?: ExplainTree[];
}
// Autocompletion strategy that wraps the main one and enriches
// it with past query items.
export class HistoryCompleteStrategy implements CompleteStrategy {
  private complete: CompleteStrategy;
  private queryHistory: string[];
  constructor(complete: CompleteStrategy, queryHistory: string[]) {
    this.complete = complete;
    this.queryHistory = queryHistory;
  }

  promQL(context: CompletionContext): Promise<CompletionResult | null> | CompletionResult | null {
    return Promise.resolve(this.complete.promQL(context)).then((res) => {
      const { state, pos } = context;
      const tree = syntaxTree(state).resolve(pos, -1);
      const start = res != null ? res.from : tree.from;

      if (start !== 0) {
        return res;
      }

      const historyItems: CompletionResult = {
        from: start,
        to: pos,
        options: this.queryHistory.map((q) => ({
          label: q.length < 80 ? q : q.slice(0, 76).concat('...'),
          detail: 'past query',
          apply: q,
          info: q.length < 80 ? undefined : q,
        })),
        validFor: /^[a-zA-Z0-9_:]+$/,
      };

      if (res !== null) {
        historyItems.options = historyItems.options.concat(res.options);
      }
      return historyItems;
    });
  }
}

const ExpressionInput: FC<PathPrefixProps & CMExpressionInputProps> = ({
  pathPrefix,
  value,
  onExpressionChange,
  queryHistory,
  metricNames,
  executeQuery,
  loading,
  enableAutocomplete,
  enableHighlighting,
  enableLinter,
  executeExplain,
  disableExplain,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const { theme } = useTheme();
  // (Re)initialize editor based on settings / setting changes.
  useEffect(() => {
    // Build the dynamic part of the config.
    promqlExtension
      .activateCompletion(enableAutocomplete)
      .activateLinter(enableLinter)
      .setComplete({
        completeStrategy: new HistoryCompleteStrategy(
          newCompleteStrategy({
            remote: { url: pathPrefix ? pathPrefix : '', cache: { initialMetricList: metricNames } },
          }),
          queryHistory
        ),
      });
    const dynamicConfig = [
      enableHighlighting ? syntaxHighlighting(promqlHighlighter) : [],
      promqlExtension.asExtension(),
      theme === 'dark' ? darkTheme : lightTheme,
    ];

    // Create or reconfigure the editor.
    const view = viewRef.current;
    if (view === null) {
      // If the editor does not exist yet, create it.
      if (!containerRef.current) {
        throw new Error('expected CodeMirror container element to exist');
      }

      const startState = EditorState.create({
        doc: value,
        extensions: [
          baseTheme,
          highlightSpecialChars(),
          history(),
          EditorState.allowMultipleSelections.of(true),
          indentOnInput(),
          bracketMatching(),
          closeBrackets(),
          autocompletion(),
          highlightSelectionMatches(),
          EditorView.lineWrapping,
          keymap.of([...closeBracketsKeymap, ...defaultKeymap, ...historyKeymap, ...completionKeymap, ...lintKeymap]),
          placeholder('Expression (press Shift+Enter for newlines)'),
          dynamicConfigCompartment.of(dynamicConfig),
          // This keymap is added without precedence so that closing the autocomplete dropdown
          // via Escape works without blurring the editor.
          keymap.of([
            {
              key: 'Escape',
              run: (v: EditorView): boolean => {
                v.contentDOM.blur();
                return false;
              },
            },
          ]),
          Prec.highest(
            keymap.of([
              {
                key: 'Enter',
                run: (v: EditorView): boolean => {
                  executeQuery();
                  return true;
                },
              },
              {
                key: 'Shift-Enter',
                run: insertNewlineAndIndent,
              },
            ])
          ),
          EditorView.updateListener.of((update: ViewUpdate): void => {
            onExpressionChange(update.state.doc.toString());
          }),
        ],
      });

      const view = new EditorView({
        state: startState,
        parent: containerRef.current,
      });

      viewRef.current = view;

      view.focus();
    } else {
      // The editor already exists, just reconfigure the dynamically configured parts.
      view.dispatch(
        view.state.update({
          effects: dynamicConfigCompartment.reconfigure(dynamicConfig),
        })
      );
    }
    // "value" is only used in the initial render, so we don't want to
    // re-run this effect every time that "value" changes.
    //
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enableAutocomplete, enableHighlighting, enableLinter, executeQuery, onExpressionChange, queryHistory, theme]);

  return (
    <>
      <InputGroup className="expression-input">
        <InputGroupAddon addonType="prepend">
          <InputGroupText>
            {loading ? <FontAwesomeIcon icon={faSpinner} spin /> : <FontAwesomeIcon icon={faSearch} />}
          </InputGroupText>
        </InputGroupAddon>
        <div ref={containerRef} className="cm-expression-input" />
        <InputGroupAddon addonType="append">
          <Button className="execute-btn" color="primary" onClick={executeQuery}>
            Execute
          </Button>
        </InputGroupAddon>
        <Button className="ml-2" color="info" onClick={executeExplain} disabled={disableExplain}>
          Explain
        </Button>
      </InputGroup>
    </>
  );
};

export default ExpressionInput;
