// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Toolbar } from '@/components/toolbar'

afterEach(() => vi.restoreAllMocks())

describe('Toolbar image tool', () => {
  it('selects the image tool and opens the picker on U', () => {
    // The picker must open from the key handler itself, or browsers block it.
    const click = vi.spyOn(HTMLInputElement.prototype, 'click').mockImplementation(() => {})
    const onSelect = vi.fn()
    render(<Toolbar locked={false} tool="pencil" onSelect={onSelect} onFile={vi.fn()} />)

    fireEvent.keyDown(window, { key: 'u' })

    expect(onSelect).toHaveBeenCalledWith('image')
    expect(click).toHaveBeenCalledTimes(1)
  })

  it('selects the image tool and opens the picker on button click', () => {
    // Clicking the icon is the main way in for new visitors.
    const click = vi.spyOn(HTMLInputElement.prototype, 'click').mockImplementation(() => {})
    const onSelect = vi.fn()
    render(<Toolbar locked={false} tool="pencil" onSelect={onSelect} onFile={vi.fn()} />)

    fireEvent.click(screen.getByRole('button', { name: 'Image' }))

    expect(onSelect).toHaveBeenCalledWith('image')
    expect(click).toHaveBeenCalledTimes(1)
  })

  it('does not open the picker for other tools', () => {
    // A picker popping up on the pencil key would block drawing.
    const click = vi.spyOn(HTMLInputElement.prototype, 'click').mockImplementation(() => {})
    render(<Toolbar locked={false} tool="pan" onSelect={vi.fn()} onFile={vi.fn()} />)

    fireEvent.keyDown(window, { key: 'p' })

    expect(click).not.toHaveBeenCalled()
  })

  it('hands the picked file over and resets the input', async () => {
    // Without the reset, picking the same file twice fires no change event.
    const onFile = vi.fn()
    render(<Toolbar locked={false} tool="image" onSelect={vi.fn()} onFile={onFile} />)
    const input = screen.getByLabelText('Image file') as HTMLInputElement
    const file = new File([new Uint8Array([1])], 'logo.png', { type: 'image/png' })

    await userEvent.upload(input, file)

    expect(onFile).toHaveBeenCalledWith(file)
    expect(input.value).toBe('')
  })
})

describe('Toolbar locked for guests', () => {
  it('enables only pan for a guest', () => {
    // Guests may not paint, so every tool that leads to a pixel must be unusable before login.
    render(<Toolbar locked tool="pan" onSelect={vi.fn()} onFile={vi.fn()} />)
    expect(screen.getByRole('button', { name: 'Pan' })).toBeEnabled()
    for (const label of ['Pencil', 'Line', 'Rectangle', 'Ellipse', 'Eyedropper', 'Image']) {
      expect(screen.getByRole('button', { name: label })).toBeDisabled()
    }
  })

  it('ignores every paint shortcut for a guest but still takes pan', () => {
    // A shortcut that slipped past the lock would hand a guest a paint tool the buttons refuse.
    const onSelect = vi.fn()
    const click = vi.spyOn(HTMLInputElement.prototype, 'click').mockImplementation(() => undefined)
    render(<Toolbar locked tool="pan" onSelect={onSelect} onFile={vi.fn()} />)

    for (const key of ['p', 'l', 'r', 'e', 'i', 'u']) fireEvent.keyDown(window, { key })
    expect(onSelect).not.toHaveBeenCalled()
    expect(click).not.toHaveBeenCalled()

    fireEvent.keyDown(window, { key: 'h' })
    expect(onSelect).toHaveBeenCalledWith('pan')
  })

  it('enables every tool once logged in', () => {
    // The lock must lift on login, or a signed-in painter is stuck with the hand.
    render(<Toolbar locked={false} tool="pan" onSelect={vi.fn()} onFile={vi.fn()} />)
    for (const label of ['Pan', 'Pencil', 'Line', 'Rectangle', 'Ellipse', 'Eyedropper', 'Image']) {
      expect(screen.getByRole('button', { name: label })).toBeEnabled()
    }
  })
})
