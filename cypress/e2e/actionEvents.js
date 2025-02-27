const createAction = (actionName) => {
    cy.get('[data-attr=create-action]').click()
    cy.get('.LemonButton').should('contain', 'From event or pageview')
    cy.get('[data-attr=new-action-pageview]').click()
    cy.get('[data-attr=action-name-create]').should('exist')

    cy.get('[data-attr=action-name-create]').type(actionName)
    cy.get('.ant-radio-group > :nth-child(3)').click()
    cy.get('[data-attr=edit-action-url-input]').click().type(Cypress.config().baseUrl)

    cy.get('[data-attr=save-action-button]').click()

    cy.contains('Calculated event saved').should('exist')
}

function navigateToEventsTab() {
    cy.clickNavMenu('datamanagement')
    cy.get('[data-attr=data-management-events-tab]').click()
}

describe('Action Events', () => {
    let actionName
    beforeEach(() => {
        navigateToEventsTab()
        actionName = Cypress._.random(0, 1e6)
    })

    it('Create action event', () => {
        createAction(actionName)

        // Test the action is immediately available
        cy.clickNavMenu('insight')

        cy.contains('Add graph series').click()
        cy.get('[data-attr=taxonomic-filter-searchfield]').type(actionName)
        cy.get('[data-attr=taxonomic-tab-actions]').click()
        cy.get('[data-attr=prop-filter-actions-0]').click()
        cy.get('[data-attr=trend-element-subject-1] span').should('contain', actionName)
    })

    it('Notifies when an action event with this name already exists', () => {
        createAction(actionName)
        navigateToEventsTab()
        createAction(actionName)

        // Oh noes, there already is an action with name `actionName`
        cy.contains('Calculated event with this name already exists').should('exist')
        // Let's see it
        cy.contains('Click here to edit').click()
        // We should now be seeing the action from "Create action"
        cy.get('[data-attr=edit-action-url-input]').should('have.value', Cypress.config().baseUrl)
    })

    it('Click on an action event', () => {
        cy.get('[data-attr=events-definition-table]').should('exist')
        cy.get('[data-attr=event-type-filter]').click()
        cy.get('[data-attr=event-type-option-action-event]').click()
        cy.get(
            '[data-row-key="0"] > .definition-column-name > .definition-column-name-content > :nth-child(1) > a'
        ).click()
        cy.get('[data-attr=action-name-edit]').should('exist')
    })
})
