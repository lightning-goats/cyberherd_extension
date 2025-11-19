// Cyberherd Extension - create app and let LNbits init-app.js mount/plugins
window.app = Vue.createApp({
  el: '#vue',
  mixins: [window.windowMixin],
  data() {
    return {
      cyberherdData: {
        form: {
          source_wallet: null,
          zap_wallet: null,
          zap_wallet_alias: '',
          zap_wallet_alias_auto: false,
          max_members: 3,
          minimum_sats: 10,
          feeder_trigger_sats: null,
          member_allocation_percent: 10,
          send_splits_enabled: false,
          tracked_tags: '#CyberHerd',
          zap_tracking_enabled: false,
          repost_tracking_enabled: false,
          likes_tracking_enabled: false,
          tracked_event_ids: [],
          midnight_reset_enabled: false,
          nip05_verification_enabled: true,
          // zap_monitor_mode deprecated (always 'payment')
          nostr_private_key: '',
          nostr_private_key_mask: '',
          nostr_private_key_set: false,
          effective_nostr_pubkey: '',
          effective_nostr_pubkey: '',
          effective_nostr_npub: ''
        },
        members: [],
        shares: [],
        allocationModel: 'none',
        zapWallet: null,
        noteIds: [],
        zapMonitorStatus: {
          running: false,
          last_message_at: null,
          last_zap_at: null,
          last_error: null,
          mode: 'payment',
          monitoring_available: true
        },
        columns: [
          { name: 'display_name', label: 'Name', field: 'display_name' },
          { name: 'lud16', label: 'LUD16', field: 'lud16' },
          { name: 'kinds', label: 'Kinds', field: row => (row.kinds ? (Array.isArray(row.kinds) ? row.kinds.join(',') : String(row.kinds)) : '-') },
          { name: 'percent', label: 'Share', field: row => (typeof row.splits_percent !== 'undefined' && row.splits_percent !== null ? (row.splits_percent + '%') : '-') },
          { name: 'amount', label: 'Amount', field: row => (row.is_active ? ((typeof row.amount !== 'undefined' && row.amount !== null) ? (row.amount + ' sats') : '0 sats') : '-') },
          { name: 'is_active', label: 'Active', field: row => (row.is_active ? 'Yes' : 'No') },
          { name: 'banned', label: 'Banned', field: row => (row.banned ? 'Yes' : 'No') }
        ],
        ui: {
          showMemberDialog: false,
          editingPubkey: null,
          newMember: { pubkey: '', amount: 0, tracked_event_id: '' },
          nostrclientAvailable: true
        },
        adminAuthFailed: false
      },
      selectedWallet: null
    }
  },
  watch: {
    selectedWallet(newW) {
      try {
        if (newW && newW.id) {
          this.cyberherdData.form.source_wallet = newW.id
        }
      } catch (e) { }
    },
    'cyberherdData.form.zap_wallet'(newId) {
      try {
        const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
        const zw = wallets.find(w => w.id === newId)
        if (zw) {
          this.cyberherdData.form.zap_wallet_alias = zw.name || ''
          this.cyberherdData.form.zap_wallet_alias_auto = true
        }
      } catch (e) { }
    }
  },
  methods: {
    // Helper to compute API base
    cyberherdApiBase() {
      if (!this._cyberherdResolvedBase) {
        // Initial heuristic
        if (typeof window === 'undefined' || !window.location) {
          this._cyberherdResolvedBase = '/extensions/cyberherd'
        } else {
          const pathname = window.location.pathname
          // Prefer direct /cyberherd if UI served there; else default to /extensions path
          this._cyberherdResolvedBase = pathname.startsWith('/cyberherd') ? '/cyberherd' : '/extensions/cyberherd'
        }
      }
      return this._cyberherdResolvedBase
    },
    async _cyberherdFetch(method, path, key, body) {
      // Centralized fetch with 404 fallback base swap
      const doReq = async () => {
        return await LNbits.api.request(method, `${this.cyberherdApiBase()}${path}`, key || null, body)
      }
      try {
        return await doReq()
      } catch (e) {
        try {
          if (e && e.response && e.response.status === 404) {
            // Flip base once: if currently /cyberherd try /extensions/cyberherd, else vice versa
            const prev = this._cyberherdResolvedBase
            const alt = prev === '/cyberherd' ? '/extensions/cyberherd' : '/cyberherd'
            if (prev !== alt) {
              this._cyberherdResolvedBase = alt
              try {
                return await doReq()
              } catch (e2) {
                throw e2
              }
            }
          }
        } catch (_) { }
        throw e
      }
    },
    findInvoiceKeyForWalletId(walletId) {
      try {
        const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
        if (walletId) {
          const found = wallets.find(w => w.id === walletId)
          if (found) return found.inkey || null
        }
        const first = wallets.find(w => w && w.inkey)
        return first ? first.inkey : null
      } catch (e) {
        return null
      }
    },
    getAuthKey(walletId) {
      // For writes, LNbits expects admin key; reads can use inkey.
      if (this.selectedWallet) {
        return this.selectedWallet.adminkey || null
      }
      const selected = walletId || (this.cyberherdData?.form?.source_wallet) || null
      const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
      const match = wallets.find(w => w.id === selected)
      if (match && match.adminkey) {
        return match.adminkey
      }
      // Fallback: find any wallet with admin key
      const fallback = wallets.find(w => w && w.adminkey)
      return fallback ? fallback.adminkey : null
    },
    getReadKey() {
      if (this.selectedWallet) return this.selectedWallet.inkey || null
      const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
      const first = wallets.find(w => w.inkey)
      return first ? first.inkey : null
    },
    async fetchSettings() {
      try {
        if (typeof LNbits === 'undefined' || !LNbits.api) {
          setTimeout(() => this.fetchSettings(), 200)
          return
        }

        try {
          const { data } = await LNbits.api.request('GET', '/api/v1/extension')
          const installed = Array.isArray(data) ? data.map(e => e.code) : []
          this.cyberherdData.ui.nostrclientAvailable = installed.includes('nostrclient')
        } catch (e) {
          this.cyberherdData.ui.nostrclientAvailable = false
        }

        const res = await this._cyberherdFetch('GET', '/api/v1/settings', this.getReadKey() || null)
        const data = res.data

        if (data && typeof data.nostrclient_available !== 'undefined') {
          this.cyberherdData.ui.nostrclientAvailable = !!data.nostrclient_available
        }

        Object.assign(this.cyberherdData.form, {
          source_wallet: data.source_wallet || null,
          zap_wallet: data.zap_wallet || null,
          zap_wallet_alias: data.zap_wallet_alias || '',
          herd_wallet: data.herd_wallet || null,
          max_members: data.max_members || 3,
          minimum_sats: data.minimum_sats || 10,
          feeder_trigger_sats: (typeof data.feeder_trigger_sats !== 'undefined' && data.feeder_trigger_sats !== null)
            ? data.feeder_trigger_sats
            : null,
          send_splits_enabled: typeof data.send_splits_enabled !== 'undefined' ? !!data.send_splits_enabled : false,
          member_allocation_percent: data.member_allocation_percent || 10,
          tracked_tags: (data.tracked_tags || []).join(', '),
          zap_tracking_enabled: data.zap_tracking_enabled || false,
          midnight_reset_enabled: typeof data.midnight_reset_enabled !== 'undefined' ? !!data.midnight_reset_enabled : false,
          nip05_verification_enabled: typeof data.nip05_verification_enabled !== 'undefined' ? !!data.nip05_verification_enabled : true,
          websocket_url: data.websocket_url || null,
          repost_tracking_enabled: data.repost_tracking_enabled || false,
          likes_tracking_enabled: data.likes_tracking_enabled || false,
          // Prefer server-provided tracked_event_ids; fall back to legacy manual_event_ids
          tracked_event_ids: data.tracked_event_ids || data.manual_event_ids || [],
          // deprecated field ignored; server always returns 'payment'
          nostr_private_key_set: !!data.nostr_private_key_set,
          nostr_private_key_mask: data.nostr_private_key_mask || '',
          nostr_private_key: '',
          effective_nostr_pubkey: data.effective_nostr_pubkey || '',
          effective_nostr_npub: data.effective_nostr_npub || ''
        })

        // Sync tracked event IDs (migrating legacy manual_event_ids if present)
        if (Array.isArray(data.tracked_event_ids) && data.tracked_event_ids.length) {
          localStorage.setItem('cyberherd_tracked_event_ids', data.tracked_event_ids.join(','))
        } else if (Array.isArray(data.manual_event_ids) && data.manual_event_ids.length) {
          // migrate legacy server field into unified key
          localStorage.setItem('cyberherd_tracked_event_ids', data.manual_event_ids.join(','))
        }

        // If nostr mode is selected but nostrclient is not available, fall back to payment mode
        // zap_monitor_mode no longer selectable; always payment

        // Sync selectedWallet to current form source wallet or default to first
        const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
        const match = wallets.find(w => w.id === this.cyberherdData.form.source_wallet)
        this.selectedWallet = match || wallets[0] || null
        // Initialize alias from zap wallet name if available; otherwise from selected wallet
        const zw = wallets.find(w => w.id === this.cyberherdData.form.zap_wallet)
        if (zw) {
          this.cyberherdData.form.zap_wallet_alias = zw.name || ''
          this.cyberherdData.form.zap_wallet_alias_auto = true
        } else if (!this.cyberherdData.form.zap_wallet_alias && this.selectedWallet) {
          this.cyberherdData.form.zap_wallet_alias = this.selectedWallet.name || ''
          this.cyberherdData.form.zap_wallet_alias_auto = true
        } else {
          this.cyberherdData.form.zap_wallet_alias_auto = false
        }
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to fetch cyberherd settings', e)
      }
    },
    async saveSettings() {
      try {
        const sourceId = this.selectedWallet?.id || this.cyberherdData.form.source_wallet
        // Always set alias to the chosen Predefined wallet's name (fallback to selected wallet)
        const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
        const zw = wallets.find(w => w.id === this.cyberherdData.form.zap_wallet)
        const alias = (zw && zw.name) ? zw.name : ((this.selectedWallet && this.selectedWallet.name) ? this.selectedWallet.name : (this.cyberherdData.form.zap_wallet_alias || ''))
        const triggerInput = this.cyberherdData.form.feeder_trigger_sats
        let feederTriggerValue = null
        if (triggerInput !== null && triggerInput !== '' && typeof triggerInput !== 'undefined') {
          const parsed = Number(triggerInput)
          if (Number.isFinite(parsed) && parsed >= 0) {
            feederTriggerValue = parsed
          }
        }
        const payload = {
          source_wallet: sourceId,
          zap_wallet: this.cyberherdData.form.zap_wallet || null,
          zap_wallet_alias: alias,
          herd_wallet: this.cyberherdData.form.herd_wallet || null,
          max_members: Number(this.cyberherdData.form.max_members) || 3,
          minimum_sats: Number(this.cyberherdData.form.minimum_sats) || 10,
          feeder_trigger_sats: feederTriggerValue,
          member_allocation_percent: Number(this.cyberherdData.form.member_allocation_percent) || 10,
          send_splits_enabled: this.cyberherdData.form.send_splits_enabled,
          tracked_tags: this.cyberherdData.form.tracked_tags.split(',').map(s => s.trim()).filter(Boolean),
          zap_tracking_enabled: this.cyberherdData.form.zap_tracking_enabled,
          repost_tracking_enabled: this.cyberherdData.form.repost_tracking_enabled,
          likes_tracking_enabled: this.cyberherdData.form.likes_tracking_enabled,
          nip05_verification_enabled: this.cyberherdData.form.nip05_verification_enabled,
          // zap_monitor_mode removed
        }

        // Nostr key handling:
        // - non-empty string => set/replace
        // - null => clear on server
        // - empty string => keep existing (omit from payload)
        if (this.cyberherdData.form.nostr_private_key === null) {
          payload.nostr_private_key = null
        } else if (this.cyberherdData.form.nostr_private_key !== '') {
          payload.nostr_private_key = this.cyberherdData.form.nostr_private_key
        }

        // Follow LNbits patterns (splitpayments/tpos): don't pre-validate via a
        // separate endpoint. Save settings directly using the selected wallet's
        // admin key.

        // Ensure we have an admin key - try selectedWallet first, then any wallet
        const authKey = this.getAuthKey() || this.getAuthKey(payload.source_wallet)
        if (!authKey) {
          this.$q.notify({ type: 'negative', message: 'No admin key available. Please select a wallet with admin privileges.' })
          return
        }

        console.log('Cyberherd saveSettings: API call to', `${this.cyberherdApiBase()}/api/v1/settings`)
        console.log('Cyberherd saveSettings: payload', payload)
        console.log('Cyberherd saveSettings: authKey present', !!authKey)

        const putRes = await this._cyberherdFetch('PUT', '/api/v1/settings', authKey, payload)
        // Immediately reflect effective pubkey/npub from server response
        try {
          const ed = (putRes && putRes.data) ? putRes.data : {}
          if (Object.prototype.hasOwnProperty.call(ed, 'effective_nostr_pubkey')) {
            this.cyberherdData.form.effective_nostr_pubkey = ed.effective_nostr_pubkey || ''
            this.cyberherdData.form.effective_nostr_npub = ed.effective_nostr_npub || ''
            this.cyberherdData.form.nostr_private_key_set = !!ed.effective_nostr_pubkey
            // Clear the input so we don't keep secrets in memory/UI after save
            this.cyberherdData.form.nostr_private_key = ''
          }
        } catch (e) { }
        await this.refreshMembers()
        await this.fetchSettings()
        await this.fetchZapMonitorStatus()

        this.$q.notify({ type: 'positive', message: 'Settings saved' })
      } catch (e) {
        console.error('Cyberherd saveSettings: Error occurred', e)
        if (e && e.response) {
          console.error('Cyberherd saveSettings: Response status', e.response.status)
          console.error('Cyberherd saveSettings: Response data', e.response.data)
        }
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to save settings', e)
        LNbits.utils.notifyApiError(e)
      }
    },
    async saveToggleSettings() {
      // Save only the toggle settings without requiring full form validation
      try {
        const sourceId = this.selectedWallet?.id || this.cyberherdData.form.source_wallet
        const payload = {
          source_wallet: sourceId,
          zap_tracking_enabled: this.cyberherdData.form.zap_tracking_enabled,
          repost_tracking_enabled: this.cyberherdData.form.repost_tracking_enabled,
          likes_tracking_enabled: this.cyberherdData.form.likes_tracking_enabled,
          // Persist midnight reset opt-in
          midnight_reset_enabled: !!this.cyberherdData.form.midnight_reset_enabled,
          nip05_verification_enabled: !!this.cyberherdData.form.nip05_verification_enabled,
          send_splits_enabled: !!this.cyberherdData.form.send_splits_enabled,
        }

        const authKey = this.getAuthKey()
        if (!authKey) {
          // If no auth key, just update the local form but don't save to server
          return
        }

        await this._cyberherdFetch('PUT', '/api/v1/settings', authKey, payload)
        this.$q.notify({ type: 'positive', message: 'Setting saved' })
      } catch (e) {
        console.error('Failed to save toggle settings', e)
        LNbits.utils.notifyApiError(e)
      }
    },
    clearNostrPrivateKey() {
      // Mark for clearing on next save
      this.cyberherdData.form.nostr_private_key = null
      this.cyberherdData.form.nostr_private_key_set = false
      this.cyberherdData.form.nostr_private_key_mask = ''
      this.cyberherdData.form.effective_nostr_pubkey = ''
      this.cyberherdData.form.effective_nostr_npub = ''
      this.$q.notify({ type: 'info', message: 'Nostr private key will be cleared on Save.' })
    },
    // removed validateSourceWallet pre-check; not needed in standard patterns
    async fetchMembers() {
      try {
        if (this.cyberherdData.adminAuthFailed) return
        const res = await this._cyberherdFetch('GET', '/api/v1/members', this.getReadKey() || null)
        if (res.data && res.data.members) {
          this.cyberherdData.members = res.data.members
        }
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to fetch cyberherd members', e)
      }
    },
    async fetchShares() {
      try {
        if (this.cyberherdData.adminAuthFailed) return
        const res = await this._cyberherdFetch('GET', '/api/v1/shares', this.getReadKey() || null)
        if (res.data) {
          this.cyberherdData.shares = Array.isArray(res.data.shares) ? res.data.shares : []
          this.cyberherdData.allocationModel = res.data.allocation_model || 'none'
          this.cyberherdData.zapWallet = res.data.zap_wallet || null
        }
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to fetch cyberherd shares', e)
      }
    },
    async fetchTodayNotes() {
      try {
        if (this.cyberherdData.adminAuthFailed) return
        const res = await this._cyberherdFetch('GET', '/api/v1/today_notes', this.getReadKey() || null)
        if (res.data && res.data.note_ids) {
          this.cyberherdData.noteIds = res.data.note_ids
        }
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to fetch today\'s notes', e)
      }
    },
    async removeMember(pubkey) {
      if (!confirm('Remove member ' + pubkey + '?')) return
      try {
        await this._cyberherdFetch('DELETE', '/api/v1/members/' + encodeURIComponent(pubkey), this.getAuthKey() || null)
        await this.refreshMembers()
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to remove member', e)
        LNbits.utils.notifyApiError(e)
      }
    },
    async activateMember(pubkey) {
      try {
        await this._cyberherdFetch('POST', '/api/v1/members/' + encodeURIComponent(pubkey) + '/activate', this.getAuthKey() || null, {})
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Member activated' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    async deactivateMember(pubkey) {
      try {
        await this._cyberherdFetch('POST', '/api/v1/members/' + encodeURIComponent(pubkey) + '/deactivate', this.getAuthKey() || null)
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Member deactivated' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    async banMember(pubkey) {
      try {
        await this._cyberherdFetch('POST', '/api/v1/members/' + encodeURIComponent(pubkey) + '/ban', this.getAuthKey() || null)
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Member banned' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    async unbanMember(pubkey) {
      try {
        await this._cyberherdFetch('POST', '/api/v1/members/' + encodeURIComponent(pubkey) + '/unban', this.getAuthKey() || null)
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Member unbanned' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    async refreshMembers() {
      try {
        const authKey = this.getAuthKey()
        if (authKey) {
          // Trigger backend recovery of today's notes, zaps, and kinds 6/7 before refreshing UI
          try {
            // Show a modal spinner while recovery runs
            this.$q.dialog({
              component: {
                template: `<div style="padding:18px;text-align:center;"><q-spinner-dots size="48px"/><div style="margin-top:12px">Recovering qualifying events and zaps...</div></div>`
              },
              persistent: true
            })
            const res = await this._cyberherdFetch('POST', '/api/v1/recover_events', authKey, {})
            // Close dialog
            try { this.$q.dialog().hide() } catch (e) { }
            // If diagnostics returned, show a friendly summary and an expandable details view
            if (res && res.data && res.data.diagnostics) {
              const d = res.data.diagnostics
              const msgs = []
              try {
                if (d.events_recovered && typeof d.events_recovered.total === 'number') {
                  const notes = d.events_recovered.notes || 0
                  const engagement = d.events_recovered.reposts || 0
                  msgs.push(`${d.events_recovered.total} events recovered (${notes} notes, ${engagement} engagement)`)
                }
                if (d.events_recovered && d.events_recovered.errors && d.events_recovered.errors.length) {
                  msgs.push(`${d.events_recovered.errors.length} event recovery errors`)
                }
                if (d.zaps && typeof d.zaps.processed === 'number') msgs.push(`${d.zaps.processed} zaps processed`)
                if (d.zaps && d.zaps.errors && d.zaps.errors.length) msgs.push(`${d.zaps.errors.length} zap errors`)
              } catch (e) { }
              const summary = msgs.length ? msgs.join(', ') : 'Recovery completed'
              this.$q.notify({ type: 'positive', message: summary })

              // Prepare pretty JSON and simple HTML-escape to safely embed inside dialog
              const pretty = JSON.stringify(d, null, 2) || ''
              const escaped = pretty.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')

              // Render a compact friendly dialog with an expandable details (<details>) element
              this.$q.dialog({
                component: {
                  template: `
                    <div style="padding:12px; max-width:680px; font-family: Arial, sans-serif;">
                      <div style="margin-bottom:8px; font-weight:600;">Recovery summary</div>
                      <div style="margin-bottom:12px; color:#333">${summary}</div>
                      <div style="margin-bottom:8px; color:#666; font-size:0.9em">If you need more information, expand the details below.</div>
                      <details style="border-top:1px solid #eee; padding-top:8px">
                        <summary style="cursor:pointer;">Show diagnostics details</summary>
                        <pre style="white-space:pre-wrap; background:#f9f9f9; padding:10px; border-radius:4px; max-height:360px; overflow:auto;">${escaped}</pre>
                      </details>
                    </div>
                  `
                },
                persistent: true,
                ok: true
              })
            } else {
              this.$q.notify({ type: 'positive', message: 'Recovery completed' })
            }
          } catch (e) {
            // Close dialog if open
            try { this.$q.dialog().hide() } catch (err) { }
            // Non-fatal: recovery may not be available in some deployments
            console.warn('Recovery endpoint call failed', e)
            this.$q.notify({ type: 'negative', message: 'Recovery failed (see console)' })
          }
        }
      } catch (e) {
        console.warn('Failed to invoke recovery endpoint', e)
      }

      // The /api/v1/recover_events endpoint handles fetchTodayNotes internally,
      // but we still need to refresh the UI state after recovery completes
      await this.fetchMembers()
      await this.fetchShares()
      await this.fetchTodayNotes()
    },
    openAddDialog() {
      this.cyberherdData.ui.editingPubkey = null
      this.cyberherdData.ui.newMember = { pubkey: '', amount: 0, tracked_event_id: '' }
      this.cyberherdData.ui.showMemberDialog = true
    },
    openEditDialog(pubkey) {
      this.cyberherdData.ui.editingPubkey = pubkey
      const curr = (this.cyberherdData.members || []).find(m => m.pubkey === pubkey) || {}
      this.cyberherdData.ui.newMember = {
        pubkey,
        amount: Number(curr.amount || 0),
        tracked_event_id: (curr.event_id || curr.note || '').toLowerCase()
      }
      this.cyberherdData.ui.showMemberDialog = true
    },
    closeDialog() {
      this.cyberherdData.ui.showMemberDialog = false
    },
    async saveMember() {
      const payload = { pubkey: this.cyberherdData.ui.newMember.pubkey }
      if (!this.cyberherdData.ui.editingPubkey) {
        const amt = Number(this.cyberherdData.ui.newMember.amount)
        if (!isNaN(amt) && amt >= 0) payload.amount = amt
        const tracked = (this.cyberherdData.ui.newMember.tracked_event_id || '').trim()
        if (tracked) payload.tracked_event_id = tracked
      }
      try {
        if (this.cyberherdData.ui.editingPubkey) {
          // Refresh metadata and apply amount
          const amt = Number(this.cyberherdData.ui.newMember.amount)
          const body = (!isNaN(amt) && amt >= 0) ? { amount: amt } : {}
          const tracked = (this.cyberherdData.ui.newMember.tracked_event_id || '').trim()
          if (tracked) body.tracked_event_id = tracked
          await this._cyberherdFetch('PUT', '/api/v1/members/' + encodeURIComponent(this.cyberherdData.ui.editingPubkey), this.getAuthKey() || null, body)
        } else {
          await this._cyberherdFetch('POST', '/api/v1/members', this.getAuthKey() || null, payload)
        }
        this.closeDialog()
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Member saved' })
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to save member', e)
        LNbits.utils.notifyApiError(e)
      }
    },
    showAdminBanner() {
      try {
        if (document.getElementById('cyberherd-admin-banner')) return
        const banner = document.createElement('div')
        banner.id = 'cyberherd-admin-banner'
        banner.style.cssText = 'background:#ffe6e6;border:1px solid #ff9999;padding:12px;margin:8px 0;font-family:Arial,sans-serif;color:#600;'
        banner.innerText = 'Cyberherd admin: authentication required. Please open LNbits as an admin (or supply an inkey) and reload this page.'
        const container = document.querySelector('main') || document.body
        container.insertBefore(banner, container.firstChild)

        if (!this.cyberherdData.ui.nostrclientAvailable) {
          const warn = document.createElement('div')
          warn.style.cssText = 'background:#fff6e6;border:1px solid #ffcc80;padding:12px;margin:8px 0;font-family:Arial,sans-serif;color:#8a6d3b;'
          warn.innerText = 'Cyberherd: The nostrclient extension is required. Please install/enable it from Extensions.'
          container.insertBefore(warn, banner.nextSibling)
        }
      } catch (e) {
        // ignore DOM errors
      }
    },
    async recomputeSplits() {
      try {
        await this._cyberherdFetch('POST', '/api/v1/recompute_splits', this.getAuthKey() || null)
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Recomputed split targets' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    async fetchZapMonitorStatus() {
      try {
        if (this.cyberherdData.adminAuthFailed) return
        const res = await this._cyberherdFetch('GET', '/api/v1/zap_monitor_status', this.getAuthKey() || null)
        if (res.data && res.data.status) {
          this.cyberherdData.zapMonitorStatus = res.data.status
        }
      } catch (e) {
        if (e && e.response && e.response.status === 401) {
          this.showAdminBanner()
          this.cyberherdData.adminAuthFailed = true
          return
        }
        console.error('Failed to fetch zap monitor status', e)
        // Reset to default status on error
        this.cyberherdData.zapMonitorStatus = {
          running: false,
          last_message_at: null,
          last_zap_at: null,
          last_error: null,
          mode: 'payment',
          monitoring_available: true
        }
      }
    },
    async confirmReset() {
      try {
        const ok = confirm('Reset today: deactivate all members and set splits to predefined wallet at 100%?')
        if (!ok) return
        await this._cyberherdFetch('POST', '/api/v1/manual_reset', this.getAuthKey() || null)
        await this.refreshMembers()
        this.$q.notify({ type: 'positive', message: 'Cyberherd reset completed' })
      } catch (e) {
        LNbits.utils.notifyApiError(e)
      }
    },
    // Return full websocket path clients can use to subscribe to messages for the configured herd wallet
    getWebsocketPath() {
      try {
        // Prefer canonical websocket_url provided by server
        if (this.cyberherdData.form.websocket_url) {
          return this.cyberherdData.form.websocket_url
        }
        // Fall back to deterministic local construction
        const topic = this.findInvoiceKeyForWalletId(this.cyberherdData.form.herd_wallet) || null
        if (!topic) {
          return 'WebSocket topic not available (herd wallet not configured or no invoice key)'
        }
        const loc = window.location
        const protocol = (loc.protocol === 'https:') ? 'wss:' : 'ws:'
        return `${protocol}//${loc.host}/ws/${topic}`
      } catch (e) {
        return ''
      }
    }
  },
  created() {
    // Follow splitpayments pattern: initialize selectedWallet from g.user
    try {
      const wallets = (this.g && this.g.user && Array.isArray(this.g.user.wallets)) ? this.g.user.wallets : []
      this.selectedWallet = wallets[0] || null
      if (this.selectedWallet && this.selectedWallet.id) {
        this.cyberherdData.form.source_wallet = this.cyberherdData.form.source_wallet || this.selectedWallet.id
        // Seed alias from zap wallet or selected wallet name
        const zw = wallets.find(w => w.id === this.cyberherdData.form.zap_wallet)
        if (zw) {
          this.cyberherdData.form.zap_wallet_alias = zw.name || ''
          this.cyberherdData.form.zap_wallet_alias_auto = true
        } else if (!this.cyberherdData.form.zap_wallet_alias) {
          this.cyberherdData.form.zap_wallet_alias = this.selectedWallet.name || ''
          this.cyberherdData.form.zap_wallet_alias_auto = true
        }
      }
    } catch (e) { }
    this.fetchSettings()
    this.fetchMembers()
    this.fetchShares()
    this.fetchTodayNotes()
    this.fetchZapMonitorStatus()

    // Auto-refresh data every 15 seconds (LNbits standard polling pattern)
    this._pollInterval = setInterval(() => {
      if (!document.hidden) {
        this.fetchMembers()
        this.fetchShares()
        this.fetchTodayNotes()
      }
    }, 15000)
  },
  beforeUnmount() {
    // Clean up polling interval when component is destroyed
    if (this._pollInterval) {
      clearInterval(this._pollInterval)
    }
  }
})
