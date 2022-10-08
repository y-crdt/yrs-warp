import * as Y from 'yjs'
import { yCollab } from 'y-codemirror.next'
import { EditorView, basicSetup } from "codemirror";
import { EditorState } from "@codemirror/state";
import { javascript } from '@codemirror/lang-javascript'
import {syntaxHighlighting, defaultHighlightStyle} from "@codemirror/language"
import * as random from 'lib0/random'
import {WebsocketProvider} from "y-websocket";

export const usercolors = [
    { color: '#30bced', light: '#30bced33' },
    { color: '#6eeb83', light: '#6eeb8333' },
    { color: '#ffbc42', light: '#ffbc4233' },
    { color: '#ecd444', light: '#ecd44433' },
    { color: '#ee6352', light: '#ee635233' },
    { color: '#9ac2c9', light: '#9ac2c933' },
    { color: '#8acb88', light: '#8acb8833' },
    { color: '#1be7ff', light: '#1be7ff33' }
]

// select a random color for this user
export const userColor = usercolors[random.uint32() % usercolors.length]

const doc = new Y.Doc()
const ytext = doc.getText('codemirror')

const provider = new WebsocketProvider('ws://localhost:8000', 'my-room', doc, { disableBc: true })

const undoManager = new Y.UndoManager(ytext)

provider.awareness.setLocalStateField('user', {
    name: 'Anonymous ' + Math.floor(Math.random() * 100),
    color: userColor.color,
    colorLight: userColor.light
})



const state = EditorState.create({
    doc: ytext.toString(),
    extensions: [
        javascript(),
        syntaxHighlighting(defaultHighlightStyle),
        yCollab(ytext, provider.awareness, { undoManager })
    ]
})

const view = new EditorView({ state, parent: document.querySelector('#editor') })
