import { configure } from 'enzyme';
import Adapter from '@cfaester/enzyme-adapter-react-18';
import { TextDecoder, TextEncoder } from 'util';
import { GlobalWithFetchMock } from 'jest-fetch-mock';
import 'mutationobserver-shim'; // Needed for CodeMirror.
import './globals';

configure({ adapter: new Adapter() });

Object.defineProperty(global, 'TextEncoder', { value: TextEncoder });
Object.defineProperty(global, 'TextDecoder', { value: TextDecoder });
const customGlobal: GlobalWithFetchMock = global as unknown as GlobalWithFetchMock;
customGlobal.fetch = require('jest-fetch-mock');
customGlobal.fetchMock = customGlobal.fetch;

// CodeMirror in the expression input requires this DOM API. When we upgrade react-scripts
// and the associated Jest deps, hopefully this won't be needed anymore.
document.getSelection = function () {
  return {
    addRange: function () {
      return;
    },
    removeAllRanges: function () {
      return;
    },
  } as unknown as Selection;
};
